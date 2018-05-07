package org.constellation.consensus

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.consensus.Consensus._
import org.constellation.p2p.PeerToPeer._
import org.constellation.primitives.Chain.Chain
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.ChainStateManager.{AddBlock, BlockAddedToChain, CreateBlockProposal, GetChain}
import org.constellation.util.Signed

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Try}

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

  case class GenerateGenesisBlock()
  case class EnableConsensus()
  case class DisableConsensus()
  case class RegisterP2PActor(p2pActor: ActorRef)

  // Events
  case class ProposedBlockUpdated(block: Block)
  case class GetMemPoolResponse(transactions: Seq[Transaction], round: Long)
  case class PeerMemPoolUpdated(transactions: Seq[Transaction], peer: Id, round: Long)
  case class PeerProposedBlock(block: Block, peer: Id)

  case class RequestBlockProposal(round: Long, id: Id)

  // Methods

  def getFacilitators(previousBlock: Block): Set[Id] = {
    // TODO: here is where we need to grab our random sampling fancy function

    previousBlock.clusterParticipants
  }

  def isFacilitator(facilitators: Set[Id], self: Id): Boolean = {
    facilitators.contains(self)
  }

  def getConsensusBlock(peerBlockProposals: HashMap[Long, HashMap[Id, Block]],
                        currentFacilitators: Set[Id], round: Long): Option[Block] = {

    var consensusBlock: Option[Block] = None

    if (!peerBlockProposals.contains(round)) {
      println("Peer block not contains round")
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

  def notifyFacilitators(previousBlock: Block, self: Id, fx: Id => Unit): Boolean = {
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
                                         self: Id,
                                         udpActor: ActorRef
                                       )(implicit system: ActorSystem): Boolean = {
    notifyFacilitators(previousBlock, self, (f) => {
      udpActor.udpSendToId(PeerProposedBlock(proposedBlock, self), f)
    })
  }

  def notifyFacilitatorsOfMemPool(
                                   previousBlock: Block, self: Id,
                                   transactions: Seq[Transaction], round: Long,
                                   udpActor: ActorRef)(implicit system: ActorSystem): Boolean = {
    // Send all of the facilitators our current memPoolState
    notifyFacilitators(previousBlock, self, (p) => {
      udpActor.udpSendToId(PeerMemPoolUpdated(transactions, self, round), p)
    })
  }

  // TODO : Use public keys to identify nodes along with a socket address.
  // I.e. we need to verify if the node associated with the IP is actually the right one and matches signature etc.
  def handleBlockAddedToChain(consensusRoundState: ConsensusRoundState,
                              latestBlock: Block,
                              memPoolManager: ActorRef,
                              self: ActorRef,
                              udpAddress: InetSocketAddress
                             ): ConsensusRoundState = {

    val peerMemPoolCache = consensusRoundState.copy().peerMemPools.filter(f => f._1 > latestBlock.round)
    val peerBlockProposalsCache= consensusRoundState.copy().peerBlockProposals.filter(f => f._1 > latestBlock.round)

    val updatedState = consensusRoundState.copy(proposedBlock = None,
      previousBlock = Some(latestBlock),
      currentFacilitators = Consensus.getFacilitators(latestBlock),
      peerMemPools = peerMemPoolCache,
      peerBlockProposals = peerBlockProposalsCache)

    if (updatedState.enabled) {
      // If we are a facilitator this round then begin consensus
      if (Consensus.isFacilitator(updatedState.currentFacilitators, consensusRoundState.selfId)) {
        memPoolManager ! GetMemPool(self, latestBlock.round + 1)
      }
    }

    updatedState
  }

  // TODO: need to revisit, useful for getting the initial seeded actor refs into the block for now
  def generateGenesisBlock(consensusRoundState: ConsensusRoundState,
                           chainStateManager: ActorRef, sender: ActorRef, replyTo: ActorRef,
                           udpAddress: InetSocketAddress)(implicit timeout: Timeout): ConsensusRoundState = {
    val updatedState = consensusRoundState.copy(selfPeerToPeerRef = Some(udpAddress))

    import constellation._

    val value = consensusRoundState.selfPeerToPeerActorRef.get

    // TODO: revisit this
    val ids = (value ? GetPeersID).mapTo[Seq[Id]].get()

    val refs = (ids ++ Seq(consensusRoundState.selfId)).toSet

    // TODO: add correct genesis block, temporary for testing
    val genesisBlock = Block("tempGenesisParentHash", 0, "tempSig", refs.toSet, 0, Seq())

    chainStateManager ! AddBlock(genesisBlock, replyTo)

    sender ! genesisBlock

    updatedState
  }

  def enableConsensus(consensusRoundState: ConsensusRoundState, memPoolManager: ActorRef, self: ActorRef): ConsensusRoundState = {
    val updatedState = consensusRoundState.copy(enabled = true)

    // If we are a facilitator this round then begin consensus
    if (Consensus.isFacilitator(consensusRoundState.currentFacilitators,
      consensusRoundState.selfId)) {
      // This is what starts consensus, it returns a response to actor of GetMemPoolResponse
      // Processing continues under receive in Consensus
      memPoolManager ! GetMemPool(self, 0L)
    }

    updatedState
  }

  def disableConsensus(consensusRoundState: ConsensusRoundState): ConsensusRoundState = {
    consensusRoundState.copy(enabled = false)
  }

  def handleProposedBlockUpdated(consensusRoundState: ConsensusRoundState, block: Block,
                                 udpAddress: InetSocketAddress, udpActor: ActorRef
                                )(implicit system: ActorSystem): ConsensusRoundState = {
    val updatedState = consensusRoundState.copy(proposedBlock = Some(block))

    val previousBlock: Option[Block] = updatedState.previousBlock
    val proposedBlock: Option[Block] = updatedState.proposedBlock

    if (proposedBlock.isDefined && previousBlock.isDefined) {
      Consensus.notifyFacilitatorsOfBlockProposal(previousBlock.get,
        proposedBlock.get,
        consensusRoundState.selfId,
        udpActor
      )
    }

    updatedState
  }

  def checkConsensusResult(consensusRoundState: ConsensusRoundState, round: Long,
                           chainStateManager: ActorRef, self: ActorRef): Unit = {

    val consensusBlock =
      Consensus.getConsensusBlock(consensusRoundState.peerBlockProposals,
        consensusRoundState.currentFacilitators,
        round)

    if (consensusBlock.isDefined) {
      chainStateManager ! AddBlock(consensusBlock.get, self)
    }
  }

  def handlePeerMemPoolUpdated(consensusRoundState: ConsensusRoundState, round: Long, peer: Id,
                               transactions: Seq[Transaction], chainStateManager: ActorRef, replyTo: ActorRef): ConsensusRoundState = {

    val peerMemPools =
      consensusRoundState.peerMemPools +
        (round -> (consensusRoundState.peerMemPools.getOrElse(round, HashMap()) + (peer -> transactions)))

    val updatedState = consensusRoundState.copy(peerMemPools = peerMemPools)

    // check if we have enough mem pools to create a block
    val facilitatorsWithoutMemPools = updatedState.currentFacilitators.filter(f => {
      !updatedState.peerMemPools(round).contains(f)
    })

    if (facilitatorsWithoutMemPools.isEmpty) {
      chainStateManager ! CreateBlockProposal(updatedState.peerMemPools(round), round, replyTo)
    }

    updatedState
  }

  def handlePeerProposedBlock(consensusRoundState: ConsensusRoundState,
                              replyTo: ActorRef, block: Block, peer: Id ): ConsensusRoundState = {

    val peerBlockProposals =
      consensusRoundState.peerBlockProposals +
        (block.round -> (consensusRoundState.peerBlockProposals.getOrElse(block.round, HashMap()) + (peer -> block)))

    val updatedState = consensusRoundState.copy(peerBlockProposals = peerBlockProposals)

    replyTo ! CheckConsensusResult(block.round)

    updatedState
  }

  case class ConsensusRoundState(selfPeerToPeerActorRef: Option[ActorRef] = None,
                                 selfPeerToPeerRef: Option[InetSocketAddress] = None,
                                 enabled: Boolean = false,
                                 proposedBlock: Option[Block] = None,
                                 previousBlock: Option[Block] = None,
                                 currentFacilitators: Set[Id] = Set(),
                                 peerMemPools: HashMap[Long, HashMap[Id, Seq[Transaction]]] = HashMap(0L -> HashMap()),
                                 peerBlockProposals: HashMap[Long, HashMap[Id, Block]] = HashMap(0L -> HashMap()),
                                 selfId: Id = null
                                )
}

class Consensus(memPoolManager: ActorRef, chainManager: ActorRef, keyPair: KeyPair,
                udpAddress: InetSocketAddress = new InetSocketAddress("127.0.0.1", 16180),
                udpActor: ActorRef,
                heartbeatEnabled: Boolean = false)
               (implicit timeout: Timeout) extends Actor with ActorLogging {

  implicit val sys: ActorSystem = context.system

  val logger = Logger(s"Consensus")

  private val selfId = Id(keyPair.getPublic)

  @volatile var consensusRoundState: ConsensusRoundState = ConsensusRoundState(selfId = selfId)

  /*
      val tipAgeValid = System.currentTimeMillis() > (lastBlockTime + 3000)
      if (isMaster && tipAgeValid && consensusRoundState.enabled) {
        logger.debug("Creating block")
        val txs = (memPoolManager ? GetMemPoolDirect).mapTo[ListBuffer[Transaction]].get().toArray.toSeq
        val b = Block(lastBlock.parentHash, lastBlock.height + 1, "", ids.toSet, lastBlock.height + 1, txs)
        chainManager ! b
        p2p ! Broadcast(b)
        lastBlockTime = System.currentTimeMillis()
      }
    }
  */

  /*
      facilitatorsWithoutBlockProposals.foreach {
        f =>
          if (f != selfId) {
            udpActor.udpSendToId(RequestBlockProposal(highestRound, consensusRoundState.selfId), f)
          }
      }

  */

  override def receive: Receive = {

    case RegisterP2PActor(p2pActor) =>
      consensusRoundState = consensusRoundState.copy(selfPeerToPeerActorRef = Some(p2pActor))

    case GenerateGenesisBlock() =>
      log.debug(s"generate genesis block = $consensusRoundState")

      this.synchronized{
        consensusRoundState =
          generateGenesisBlock(consensusRoundState, chainManager, sender, self, udpAddress)
      }

    case EnableConsensus() =>
     consensusRoundState = enableConsensus(consensusRoundState, memPoolManager, self)

    case DisableConsensus() =>
      consensusRoundState = disableConsensus(consensusRoundState)

    case BlockAddedToChain(latestBlock) =>
      //   log.debug(s"block added to chain = $latestBlock")

      this.synchronized {
        consensusRoundState =
          handleBlockAddedToChain(
            consensusRoundState, latestBlock, memPoolManager, self, udpAddress
          )
      }

    case ProposedBlockUpdated(block) =>
      //  log.debug(s"self proposed block updated = $block")

      this.synchronized {
        consensusRoundState = handleProposedBlockUpdated(consensusRoundState, block, udpAddress, udpActor)

        // This used to happen inside handlePeerProposed block but since we're routing those messages
        // over UDP, need to make this call directly since its a self-interaction
        val peerBlockProposals =
        consensusRoundState.peerBlockProposals +
          (block.round -> (consensusRoundState.peerBlockProposals.getOrElse(block.round, HashMap()) +
            (consensusRoundState.selfId -> block)))

        val updatedState = consensusRoundState.copy(peerBlockProposals = peerBlockProposals)
        consensusRoundState = updatedState
      }

    case g @ GetMemPoolResponse(transactions, round) =>

      // This originally is triggered by enableConsensus which starts the GetMemPool request
      // This was sent by the ChainStateManager, it will then
      // go over UDP to P2P actor, the next call is PeerMemPoolUpdated (below) on remote side.

      this.synchronized {

        if (transactions.nonEmpty) {
          //     logger.debug("GetMemPoolResponse has transactions")
        }

        val peerMemPools =
          consensusRoundState.peerMemPools +
            (round -> (consensusRoundState.peerMemPools.getOrElse(round, HashMap()) + (consensusRoundState.selfId -> transactions)))

        val updatedState = consensusRoundState.copy(peerMemPools = peerMemPools)
        consensusRoundState = updatedState

        notifyFacilitatorsOfMemPool(consensusRoundState.previousBlock.get,
          consensusRoundState.selfId, transactions, round, udpActor)
      }

    case CheckConsensusResult(round) =>

      this.synchronized {
        checkConsensusResult(consensusRoundState, round, chainManager, self)
      }

    case PeerMemPoolUpdated(transactions, peer, round) =>

      this.synchronized {
        consensusRoundState = handlePeerMemPoolUpdated(consensusRoundState, round, peer, transactions, chainManager, self)
      }

    case PeerProposedBlock(block, peer) =>
      logger.debug(s"peer proposed block = received from ${peer.short} on ${consensusRoundState.selfId.short}")

      this.synchronized {
        consensusRoundState = handlePeerProposedBlock(consensusRoundState, self, block, peer)
      }

  }

}