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
import org.constellation.primitives.Schema.GetPeersID
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
  case class PerformConsensusRound(facilitators: Set[Id], round: Long, replyTo: ActorRef)
  case class GetMemPool(replyTo: ActorRef, round: Long)
  case class CheckConsensusResult(round: Long)

  // Events
  case class ProposedBlockUpdated(block: Block)
  case class GetMemPoolResponse(transactions: Seq[Transaction], round: Long)
  case class PeerMemPoolUpdated(transactions: Seq[Transaction], peer: Id, round: Long)
  case class PeerProposedBlock(block: Block, peer: Id)
  case class ConsensusResult(block: Block, round: Long)

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

    println(s"get consensus block proposals = $peerBlockProposals, " +
      s"round = $round, facilitators = $currentFacilitators")

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

      println(s"blocks = $blocks")

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

    facilitators.filter(p => p != self).foreach(fx)

    true
  }

  def notifyFacilitatorsOfBlockProposal(previousBlock: Block,
                                        proposedBlock: Block,
                                        self: Id,
                                        udpActor: ActorRef)(implicit system: ActorSystem): Boolean = {

    // TODO: here replace with call to gossip actor

    notifyFacilitators(previousBlock, self, (f) => {
      udpActor.udpSendToId(PeerProposedBlock(proposedBlock, self), f)
    })
  }

  def notifyFacilitatorsOfMemPool(previousBlock: Block, self: Id,
                                  transactions: Seq[Transaction], round: Long,
                                  udpActor: ActorRef)(implicit system: ActorSystem): Boolean = {
    // TODO: here replace with call to gossip actor

    notifyFacilitators(previousBlock, self, (p) => {
      udpActor.udpSendToId(PeerMemPoolUpdated(transactions, self, round), p)
    })
  }

  def performConsensusRound(consensusRoundState: ConsensusRoundState,
                         round: Long,
                         facilitators: Set[Id],
                         memPoolManager: ActorRef,
                         self: ActorRef,
                         udpAddress: InetSocketAddress,
                         replyTo: ActorRef): ConsensusRoundState = {

    val peerMemPoolCache = consensusRoundState.copy().peerMemPools.filter(f => f._1 > round)
    val peerBlockProposalsCache= consensusRoundState.copy().peerBlockProposals.filter(f => f._1 > round)

    val updatedState = consensusRoundState.copy(proposedBlock = None,
      currentRound = round,
      currentFacilitators = facilitators,
      peerMemPools = peerMemPoolCache,
      replyToWithConsensusResult = Some(replyTo),
      peerBlockProposals = peerBlockProposalsCache)

    // If we are a facilitator this round then begin consensus
    if (Consensus.isFacilitator(updatedState.currentFacilitators, updatedState.selfId)) {
      memPoolManager ! GetMemPool(self, round)
    }

    updatedState
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

  def checkConsensusResult(consensusRoundState: ConsensusRoundState,
                           round: Long): Option[Block] = {

    val consensusBlock =
      Consensus.getConsensusBlock(consensusRoundState.peerBlockProposals,
        consensusRoundState.currentFacilitators,
        round)

    if (consensusBlock.isDefined && consensusRoundState.replyToWithConsensusResult.isDefined) {
      consensusRoundState.replyToWithConsensusResult.get ! ConsensusResult(consensusBlock.get, round)
    }

    consensusBlock
  }

  def handlePeerMemPoolUpdated(consensusRoundState: ConsensusRoundState,
                               round: Long,
                               peer: Id,
                               transactions: Seq[Transaction],
                               chainStateManager: ActorRef,
                               replyTo: ActorRef): ConsensusRoundState = {

    if (round < consensusRoundState.currentRound) {
      return consensusRoundState
    }

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

    if (block.round < consensusRoundState.currentRound) {
      return consensusRoundState
    }

    val peerBlockProposals =
      consensusRoundState.peerBlockProposals +
        (block.round -> (consensusRoundState.peerBlockProposals.getOrElse(block.round, HashMap()) + (peer -> block)))

    val updatedState = consensusRoundState.copy(peerBlockProposals = peerBlockProposals)

    replyTo ! CheckConsensusResult(block.round)

    updatedState
  }

  case class ConsensusRoundState(proposedBlock: Option[Block] = None,
                                 previousBlock: Option[Block] = None,
                                 currentFacilitators: Set[Id] = Set(),
                                 peerMemPools: HashMap[Long, HashMap[Id, Seq[Transaction]]] = HashMap(0L -> HashMap()),
                                 peerBlockProposals: HashMap[Long, HashMap[Id, Block]] = HashMap(0L -> HashMap()),
                                 selfId: Id = null,
                                 currentRound: Long = 0,
                                 replyToWithConsensusResult: Option[ActorRef] = None)
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

  override def receive: Receive = {

    case PerformConsensusRound(facilitators, round, replyTo) =>
      logger.debug(s"$selfId init consensus round with facilitators = $facilitators for round = $round")

      consensusRoundState =
        performConsensusRound(consensusRoundState, round, facilitators, memPoolManager, self, udpAddress, replyTo)

    case ProposedBlockUpdated(block) =>
      logger.debug(s"$selfId proposed block updated= $block")

      val peerBlockProposals =
      consensusRoundState.peerBlockProposals +
        (block.round -> (consensusRoundState.peerBlockProposals.getOrElse(block.round, HashMap()) +
          (consensusRoundState.selfId -> block)))

      consensusRoundState = consensusRoundState.copy(peerBlockProposals = peerBlockProposals)

      consensusRoundState = handleProposedBlockUpdated(consensusRoundState, block, udpAddress, udpActor)

    case GetMemPoolResponse(transactions, round) =>

      val peerMemPools =
        consensusRoundState.peerMemPools +
          (round -> (consensusRoundState.peerMemPools.getOrElse(round, HashMap()) + (consensusRoundState.selfId -> transactions)))

      consensusRoundState = consensusRoundState.copy(peerMemPools = peerMemPools)

      notifyFacilitatorsOfMemPool(consensusRoundState.previousBlock.get,
        consensusRoundState.selfId, transactions, round, udpActor)

      self ! PeerMemPoolUpdated(transactions, consensusRoundState.selfId, round)

    case CheckConsensusResult(round) =>
      logger.debug(s"$selfId check consensus result round= $round")

      val block = checkConsensusResult(consensusRoundState, round)

      logger.debug(s"$selfId check consensus result block = $block for round $round")

    case PeerMemPoolUpdated(transactions, peer, round) =>
      logger.debug(s"$selfId peer mem pool updated peer = $peer, $transactions")

      consensusRoundState = handlePeerMemPoolUpdated(consensusRoundState, round, peer, transactions, chainManager, self)

    case PeerProposedBlock(block, peer) =>
      logger.debug(s"$selfId peer proposed block = received from ${peer.short} on ${consensusRoundState.selfId.short}")

      consensusRoundState = handlePeerProposedBlock(consensusRoundState, self, block, peer)

  }

}