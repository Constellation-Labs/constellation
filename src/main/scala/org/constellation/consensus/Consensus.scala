package org.constellation.consensus

import java.net.InetSocketAddress
import java.security.{KeyPair, PrivateKey, PublicKey}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import org.constellation.consensus.Consensus._
import org.constellation.p2p.PeerToPeer.{GetPeerActorRefs, GetPeers, Peers}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.ChainStateManager.{AddBlock, BlockAddedToChain, CreateBlockProposal}
import org.constellation.wallet.KeyUtils
import akka.pattern.ask
import akka.util.Timeout
import org.constellation.p2p.UDPActor

import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import constellation._

/*

  Consensus Flow (temporary):

  - a consensus round starts

  - we grab the latest set of transactions up to some limit

  - we send each "facilitator" every set of transactions

  - on the node we keep track of a map of facilitator ref to set of transactions

  - we take a union of the transactions

  - we create a block

  - we send the block to each of the nodes again

  - we keep track of the map of facilitator to blocks

  - once we have all of them, we compare the blocks and if they are all the same then we add the block to our local chain

  - we remove the transactions in the block from our mem pool

  - start the process again

  - if they are not the same then we reset the state and start the process again

 */
object Consensus {

  // Commands
  case class GetMemPool(replyTo: ActorRef, round: Long)
  case class CheckConsensusResult(round: Long)

  case class GenerateGenesisBlock(selfRemoteActorRef: ActorRef)
  case class EnableConsensus()

  // Events
  case class ProposedBlockUpdated(block: Block)
  case class MemPoolUpdated(transactions: Seq[Transaction], round: Long)
  case class PeerMemPoolUpdated(transactions: Seq[Transaction], peer: InetSocketAddress, round: Long)
  case class PeerProposedBlock(block: Block, peer: InetSocketAddress)

  // Methods

  def getFacilitators(previousBlock: Block): Set[InetSocketAddress] = {
    // TODO: here is where we need to grab our random sampling fancy function

    previousBlock.clusterParticipants
  }

  def isFacilitator(facilitators: Set[InetSocketAddress], self: InetSocketAddress): Boolean = {
    facilitators.contains(self)
  }

  def getConsensusBlock(peerBlockProposals: HashMap[Long, HashMap[InetSocketAddress, Block]],
                        currentFacilitators: Set[InetSocketAddress], round: Long): Option[Block] = {

    var consensusBlock: Option[Block] = None

    if (!peerBlockProposals.contains(round)) {
      return consensusBlock
    }

    val facilitatorsWithoutBlockProposals = currentFacilitators.filter(f => {
      !peerBlockProposals(round).contains(f)
    })

    if (facilitatorsWithoutBlockProposals.isEmpty) {

      val blocks = peerBlockProposals(round).values

      // TODO: update to be from a threshold not all
      val allBlocksInConsensus = blocks.toList.distinct.length == 1

      if (allBlocksInConsensus) {
        consensusBlock = Some(blocks.head)
      }
    }

    consensusBlock
  }

  def notifyFacilitators(previousBlock: Block, self: InetSocketAddress, fx: InetSocketAddress => Unit): Boolean = {
    val facilitators = getFacilitators(previousBlock)

    // make sure that we are a facilitator
    if (!isFacilitator(facilitators, self)) {
      return false
    }

    facilitators.foreach(fx)

    true
  }

  def notifyFacilitatorsOfBlockProposal(
                                         previousBlock: Block,
                                         proposedBlock: Block,
                                         self: InetSocketAddress,
                                         udpActor: ActorRef
                                       )(implicit system: ActorSystem): Boolean = {
    notifyFacilitators(previousBlock, self, (f) => {
      udpActor.udpSend(PeerProposedBlock(proposedBlock, self), f)
    })
  }

  def notifyFacilitatorsOfPeerMemPoolUpdated(
                                              previousBlock: Block, self: InetSocketAddress,
                                             transactions: Seq[Transaction], round: Long,
                                              udpActor: ActorRef)(implicit system: ActorSystem): Boolean = {
    // Send all of the facilitators our current memPoolState
    Consensus.notifyFacilitators(previousBlock, self, (p) => {
      udpActor.udpSend(PeerMemPoolUpdated(transactions, self, round), p)
    })
  }

  // TODO : Use public keys to identify nodes along with a socket address.
  // I.e. we need to verify if the node associated with the IP is actually the right one and matches signature etc.

  case class ConsensusRoundState(selfPeerToPeerRef: Option[ActorRef] = None,
                                 enabled: Boolean = false,
                                 proposedBlock: Option[Block] = None,
                                 previousBlock: Option[Block] = None,
                                 currentFacilitators: Set[InetSocketAddress] = Set(),
                                 peerMemPools: HashMap[Long, HashMap[InetSocketAddress, Seq[Transaction]]] = HashMap(0L -> HashMap()),
                                 peerBlockProposals: HashMap[Long, HashMap[InetSocketAddress, Block]] = HashMap(0L -> HashMap()))

}

class Consensus(memPoolManager: ActorRef, chainManager: ActorRef, keyPair: KeyPair,
                udpAddress: InetSocketAddress = new InetSocketAddress("127.0.0.1", 16180),
                udpActor: ActorRef
               )
               (implicit timeout: Timeout) extends Actor with ActorLogging {

  var consensusRoundState: ConsensusRoundState = ConsensusRoundState()

  implicit val sys: ActorSystem = context.system

  override def receive: Receive = {
    case GenerateGenesisBlock(selfRemoteActorRef) =>
      consensusRoundState = consensusRoundState.copy(selfPeerToPeerRef = Some(selfRemoteActorRef))

      log.debug(s"generate genesis block = $consensusRoundState")

      import constellation._
      // TODO: revisit this
      val seedPeerRefs = (consensusRoundState.selfPeerToPeerRef.get ? GetPeers).mapTo[Peers].get()

      val refs = seedPeerRefs.peers ++ Seq(udpAddress)

      // TODO: add correct genesis block, temporary for testing
      val genesisBlock = Block("tempGenesisParentHash", 0, "tempSig", refs.toSet, 0, Seq())

      chainManager ! AddBlock(genesisBlock)

      sender() ! genesisBlock

    case EnableConsensus() =>
      consensusRoundState = consensusRoundState.copy(enabled = true)

      log.debug(s"enable consensus request = $consensusRoundState")

      // If we are a facilitator this round then begin consensus
      if (Consensus.isFacilitator(consensusRoundState.currentFacilitators,
        udpAddress)) {
        memPoolManager ! GetMemPool(self, 0L)
      }

    case BlockAddedToChain(prevBlock) =>
      log.debug(s"block added to chain = $prevBlock")

      val peerMemPoolCache = consensusRoundState.copy().peerMemPools.filter(f => f._1 > prevBlock.round)
      val peerBlockProposalsCache = consensusRoundState.copy().peerBlockProposals.filter(f => f._1 > prevBlock.round)

      consensusRoundState = consensusRoundState.copy(proposedBlock = None,
        previousBlock = Some(prevBlock),
        currentFacilitators = Consensus.getFacilitators(prevBlock),
        peerMemPools = peerMemPoolCache,
        peerBlockProposals = peerBlockProposalsCache)

      if (consensusRoundState.enabled) {
        // If we are a facilitator this round then begin consensus
        if (Consensus.isFacilitator(consensusRoundState.currentFacilitators, udpAddress)) {
          memPoolManager ! GetMemPool(self, prevBlock.round + 1)
        }
      }

    case ProposedBlockUpdated(block) =>

      consensusRoundState = consensusRoundState.copy(proposedBlock = Some(block))

      val previousBlock = consensusRoundState.previousBlock
      val proposedBlock = consensusRoundState.proposedBlock

      if (proposedBlock.isDefined && previousBlock.isDefined) {
        Consensus.notifyFacilitatorsOfBlockProposal(
          previousBlock.get,
          proposedBlock.get,
          udpAddress,
          udpActor
        )
      }

    case MemPoolUpdated(transactions, round) =>
      Consensus.notifyFacilitatorsOfPeerMemPoolUpdated(
        consensusRoundState.previousBlock.get, udpAddress, transactions, round, udpActor)

    case CheckConsensusResult(round) =>
      val consensusBlock =
        Consensus.getConsensusBlock(consensusRoundState.peerBlockProposals,
          consensusRoundState.currentFacilitators,
          round)

      if (consensusBlock.isDefined) {
        chainManager ! AddBlock(consensusBlock.get)
      }

    case PeerMemPoolUpdated(transactions, peer, round) =>
      log.debug(s"peer mem pool updated = $transactions, $peer, $round")

      val peerMemPools =
        consensusRoundState.peerMemPools +
          (round -> (consensusRoundState.peerMemPools.getOrElse(round, HashMap()) + (peer -> transactions)))

      consensusRoundState = consensusRoundState.copy(peerMemPools = peerMemPools)

      // TODO: extract
      // check if we have enough mem pools to create a block
      val facilitatorsWithoutMemPools = consensusRoundState.currentFacilitators.filter(f => {
        !consensusRoundState.peerMemPools(round).contains(f)
      })

      if (facilitatorsWithoutMemPools.nonEmpty) {
        log.debug("Have not received enough mem pools to create a block yet")
      } else {
        chainManager ! CreateBlockProposal(consensusRoundState.peerMemPools(round), round)
      }

    case PeerProposedBlock(block, peer) =>
      log.debug(s"peer proposed block = $block, $peer")

      val peerBlockProposals =
        consensusRoundState.peerBlockProposals +
          (block.round -> (consensusRoundState.peerBlockProposals.getOrElse(block.round, HashMap()) + (peer -> block)))

      consensusRoundState = consensusRoundState.copy(peerBlockProposals = peerBlockProposals)

      self ! CheckConsensusResult(block.round)
  }

}