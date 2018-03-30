package org.constellation.consensus

import java.security.{KeyPair, PrivateKey, PublicKey}

import akka.actor.{Actor, ActorLogging, ActorRef}
import org.constellation.consensus.Consensus._
import org.constellation.p2p.PeerToPeer.{GetPeerActorRefs, GetPeers, Peers}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.ChainStateManager.{AddBlock, BlockAddedToChain, CreateBlockProposal}
import org.constellation.wallet.KeyUtils
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

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
  case class PeerMemPoolUpdated(transactions: Seq[Transaction], peer: ActorRef, round: Long)
  case class PeerProposedBlock(block: Block, peer: ActorRef)

  def getFacilitators(previousBlock: Block): Set[ActorRef] = {
    // TODO: here is where we need to grab our random sampling fancy function

    previousBlock.clusterParticipants
  }

  def isFacilitator(facilitators: Set[ActorRef], self: ActorRef): Boolean = {
    facilitators.contains(self)
  }

  def getConsensusBlock(peerBlockProposals: HashMap[Long, HashMap[ActorRef, Block]],
                        currentFacilitators: Set[ActorRef], round: Long): Option[Block] = {

    val facilitatorsWithoutBlockProposals = currentFacilitators.filter(f => {
      !peerBlockProposals(round).contains(f)
    })

    var consensusBlock: Option[Block] = None

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

  def notifyFacilitators(previousBlock: Block, self: ActorRef, fx: ActorRef => Unit): Boolean = {
    val facilitators: Set[ActorRef] = getFacilitators(previousBlock)

    // make sure that we are a facilitator
    if (!isFacilitator(facilitators, self)) {
      return false
    }

    facilitators.foreach(fx)

    true
  }

  def notifyFacilitatorsOfBlockProposal(previousBlock: Block, proposedBlock: Block, self: ActorRef): Boolean = {
    notifyFacilitators(previousBlock, self, (f) => {
      f ! PeerProposedBlock(proposedBlock, self)
    })
  }

  def notifyFacilitatorsOfPeerMemPoolUpdated(previousBlock: Block, self: ActorRef,
                                             transactions: Seq[Transaction], round: Long): Boolean = {
    // Send all of the facilitators our current memPoolState
    Consensus.notifyFacilitators(previousBlock, self, (p) => {
      p ! PeerMemPoolUpdated(transactions, self, round)
    })
  }

  case class ConsensusRoundState(proposedBlock: Option[Block] = None,
                                 previousBlock: Option[Block] = None,
                                 currentFacilitators: Set[ActorRef] = Set(),
                                 peerMemPools: HashMap[Long, HashMap[ActorRef, Seq[Transaction]]] = HashMap(0L -> HashMap()),
                                 peerBlockProposals: HashMap[Long, HashMap[ActorRef, Block]] = HashMap(0L -> HashMap()))

}

class Consensus(memPoolManager: ActorRef, chainManager: ActorRef, keyPair: KeyPair)
               (implicit timeout: Timeout) extends Actor with ActorLogging {

  var consensusRoundState: ConsensusRoundState = ConsensusRoundState()

  var selfRemoteRef: ActorRef = _

  var consensusEnabled = false

  override def receive: Receive = {
    case GenerateGenesisBlock(selfRemoteActorRef) =>

      selfRemoteRef = selfRemoteActorRef

      // TODO: revist this
      val seedPeerRefs = Await.result(selfRemoteRef ? GetPeerActorRefs, 5.seconds).asInstanceOf[Set[ActorRef]]

      // TODO: add correct genesis block, temporary for testing
      val genesisBlock = Block("tempGenesisParentHash", 0, "tempSig", seedPeerRefs.+(selfRemoteRef), 0, Seq())

      chainManager ! AddBlock(genesisBlock)

    case EnableConsensus() =>
      consensusEnabled = true

      // If we are a facilitator this round then begin consensus
      if (Consensus.isFacilitator(consensusRoundState.currentFacilitators, selfRemoteRef)) {
        memPoolManager ! GetMemPool(self, 0L)
      }

    case BlockAddedToChain(prevBlock) =>
      log.debug(s"block added to chain = $prevBlock")

      val peerMemPoolCache = consensusRoundState.copy().peerMemPools.filter(f => f._1 > prevBlock.round)
      val peerBlockProposalsCache = consensusRoundState.copy().peerBlockProposals.filter(f => f._1 > prevBlock.round)

      consensusRoundState = ConsensusRoundState(None,
        Some(prevBlock), Consensus.getFacilitators(prevBlock), peerMemPoolCache, peerBlockProposalsCache)

      if (consensusEnabled) {
        // If we are a facilitator this round then begin consensus
        if (Consensus.isFacilitator(consensusRoundState.currentFacilitators, selfRemoteRef)) {
          memPoolManager ! GetMemPool(self, prevBlock.round + 1)
        }
      }

    case ProposedBlockUpdated(block) =>

      consensusRoundState = consensusRoundState.copy(proposedBlock = Some(block))

      val previousBlock = consensusRoundState.previousBlock
      val proposedBlock = consensusRoundState.proposedBlock

      if (proposedBlock.isDefined && previousBlock.isDefined) {
        Consensus.notifyFacilitatorsOfBlockProposal(previousBlock.get, proposedBlock.get, selfRemoteRef)
      }

    case MemPoolUpdated(transactions, round) =>
      Consensus.notifyFacilitatorsOfPeerMemPoolUpdated(
        consensusRoundState.previousBlock.get, selfRemoteRef, transactions, round)

    case CheckConsensusResult(round) =>
      val consensusBlock =
        Consensus.getConsensusBlock(consensusRoundState.peerBlockProposals, consensusRoundState.currentFacilitators, round)

      if (consensusBlock.isDefined) {
        chainManager ! AddBlock(consensusBlock.get)
      }

    case PeerMemPoolUpdated(transactions, peer, round) =>
      log.debug(s"peer mem pool updated = $transactions, $peer, $round")

      consensusRoundState = consensusRoundState.copy(
        peerMemPools = consensusRoundState.peerMemPools.+(round ->
          consensusRoundState.peerMemPools(round).+(peer -> transactions)))

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

      consensusRoundState =
        consensusRoundState.copy(peerBlockProposals = consensusRoundState.peerBlockProposals.+(block.round ->
          consensusRoundState.peerBlockProposals(block.round).+(peer -> block)))

      self ! CheckConsensusResult(block.round)
  }

}