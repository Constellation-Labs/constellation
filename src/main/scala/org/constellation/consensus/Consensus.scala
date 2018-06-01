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
import org.constellation.primitives.Schema._
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.ChainStateManager.{AddBlock, BlockAddedToChain, CreateBlockProposal, GetChain}
import org.constellation.util.{ProductHash, Signed}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Try}

object Consensus {
  sealed trait CC

  sealed trait Conflict extends CC
  sealed trait Checkpoint extends CC

  sealed trait CachedData[+T <: CC]

  sealed trait VoteData[+T <: CC] extends CachedData[T]
  sealed trait ProposalData[+T <: CC] extends CachedData[T]

  case class CheckpointVote(data: Bundle) extends VoteData[Checkpoint]
  case class ConflictVote(data: Vote) extends VoteData[Conflict]

  case class CheckpointProposal(data: Bundle) extends ProposalData[Checkpoint]
  case class ConflictProposal(data: Bundle) extends ProposalData[Conflict]

  case class RoundHash[+T <: CC](hash: String)

  sealed trait RemoteMessage

  case class ConsensusVote[+T <: CC](id: Id, data: VoteData[T], roundHash: RoundHash[T]) extends RemoteMessage
  case class ConsensusProposal[+T <: CC](id: Id, data: ProposalData[T], roundHash: RoundHash[T]) extends RemoteMessage

  // TODO: do we need to gossip after receiving this message from an external node to the other nodes?
//  case class StartConsensusRound[T <: CC](id: Id, data: VoteData[T]) extends RemoteMessage

  case class InitializeConsensusRound[+T <: CC](facilitators: Set[Id], roundHash: RoundHash[T], replyTo: ActorRef)

  case class ConsensusRoundResult[+T <: CC](bundle: Bundle, roundHash: RoundHash[T])

  case class RoundState(facilitators: Set[Id] = Set(),
                        votes: HashMap[Id, _ <: VoteData[_ <: CC]] = HashMap(),
                        proposals: HashMap[Id, _ <: ProposalData[_ <: CC]] = HashMap(),
                        replyTo: Option[ActorRef] = None)

  case class ConsensusRoundState(selfId: Option[Id] = None,
                                 udpActor: Option[ActorRef] = None,
                                 roundStates: HashMap[RoundHash[_ <: CC], RoundState] = HashMap())

  // Methods
  def isFacilitator(facilitators: Set[Id], self: Id): Boolean = {
    facilitators.contains(self)
  }

  def notifyFacilitators(facilitators: Set[Id], self: Id, fx: Id => Unit): Boolean = {

    // make sure that we are a facilitator
    if (!isFacilitator(facilitators, self)) {
      return false
    }

    facilitators.filter(p => p != self).foreach(fx)

    true
  }

  def notifyFacilitatorsOfMessage(facilitators: Set[Id],
                                  self: Id,
                                  message: RemoteMessage,
                                  udpActor: ActorRef)(implicit system: ActorSystem): Boolean = {

    // TODO: here replace with call to gossip actor
    notifyFacilitators(facilitators, self, f => {
      udpActor.udpSendToId(message, f)
    })

    true
  }

  def initializeConsensusRound[T <: CC](consensusRoundState: ConsensusRoundState,
                                        facilitators: Set[Id],
                                        roundHash: RoundHash[T],
                                        replyTo: ActorRef)(implicit system: ActorSystem): ConsensusRoundState = {

    val self = consensusRoundState.selfId.get
    val udpActor = consensusRoundState.udpActor.get

    val updatedRoundStates = consensusRoundState.roundStates +
      (roundHash -> getCurrentRoundState(consensusRoundState, roundHash)
        .copy(facilitators = facilitators, replyTo = Some(replyTo)))

    val updatedState = consensusRoundState.copy(roundStates = updatedRoundStates)

    // tell everyone to perform a vote given the options
  //  notifyFacilitatorsOfMessage(facilitators, self, StartConsensusRound(self, vote), udpActor)

    updatedState
  }

  def getCurrentRoundState[T <: CC](consensusRoundState: ConsensusRoundState, roundHash: RoundHash[T]): RoundState = {
    consensusRoundState.roundStates.getOrElse(roundHash, RoundState())
  }

  def updateRoundCache[T <: CC](consensusRoundState: ConsensusRoundState,
                                peer: Id,
                                roundHash: RoundHash[T],
                                event: CachedData[T]): ConsensusRoundState = {

    val roundState = getCurrentRoundState(consensusRoundState, roundHash)

    val updatedRoundState = event match {
      case v: VoteData[T] =>
        val updatedEventCache = roundState.votes + (peer -> v)
        roundState.copy(votes = updatedEventCache)

      case p: ProposalData[T] =>
        val updatedEventCache = roundState.proposals + (peer -> p)
        roundState.copy(proposals = updatedEventCache)
    }

    val updatedRoundStates = consensusRoundState.roundStates + (roundHash -> updatedRoundState)

    consensusRoundState.copy(roundStates = updatedRoundStates)
  }

  def cleanupRoundStateCache[T <: CC](consensusRoundState: ConsensusRoundState,
                                      roundHash: RoundHash[T]): ConsensusRoundState = {
    val roundStates = consensusRoundState.roundStates.-(roundHash)
    consensusRoundState.copy(roundStates = roundStates)
  }

  def peerThresholdMet[T <: CC](consensusRoundState: ConsensusRoundState, roundHash: RoundHash[T])
                      (r: RoundState => HashMap[Id, _]): Boolean = {

    val roundState = getCurrentRoundState(consensusRoundState, roundHash)

    // TODO: update here to require a threshold, not every facilitator
    val facilitatorsMissingInfo = roundState.facilitators.filter(f => !r(roundState).contains(f))

    facilitatorsMissingInfo.isEmpty
  }

  def handlePeerVote[T <: CC](consensusRoundState: ConsensusRoundState,
                     peer: Id,
                     vote: VoteData[T],
                     roundHash: RoundHash[T])(implicit system: ActorSystem, keyPair: KeyPair): ConsensusRoundState = {

    var updatedState = updateRoundCache(consensusRoundState, peer, roundHash, vote)

    if (peerThresholdMet(updatedState, roundHash)(_.votes)) {

      val roundState = getCurrentRoundState[T](updatedState, roundHash)

      // create a bundle proposal

      // figure out what the majority of votes agreed upon
      val votes = roundState.votes

      // take those transactions bundle and sign them
      val facilitators = roundState.facilitators

      val self = consensusRoundState.selfId.get
      val udpActor = consensusRoundState.udpActor.get

      // TODO: temp logic
      val vote = votes(consensusRoundState.selfId.get)

      vote match {
        case CheckpointVote(data) =>
          val proposal = CheckpointProposal(Bundle(BundleData(data.bundleData.data.bundles).signed()(keyPair = keyPair)))

          updatedState =
            handlePeerProposedBundle(consensusRoundState, consensusRoundState.selfId.get, proposal, roundHash)

          notifyFacilitatorsOfMessage(facilitators,
            self, ConsensusProposal(self, proposal, roundHash), udpActor)
        case ConflictVote(data) =>
          val proposal = ConflictProposal(Bundle(BundleData(data.vote.data.accept).signed()(keyPair = keyPair)))

          updatedState =
            handlePeerProposedBundle(consensusRoundState, consensusRoundState.selfId.get, proposal, roundHash)

          notifyFacilitatorsOfMessage(facilitators,
            self, ConsensusProposal(self, proposal, roundHash), udpActor)
      }
    }

    // TODO: we gossip each vote?

    updatedState
  }

  def handlePeerProposedBundle[T <: CC](consensusRoundState: ConsensusRoundState,
                               peer: Id,
                               bundle: ProposalData[T],
                               roundHash: RoundHash[T]): ConsensusRoundState = {

    var updatedState = updateRoundCache(consensusRoundState, peer, roundHash, bundle)

    if (peerThresholdMet(updatedState, roundHash)(_.proposals)) {

      val roundState = getCurrentRoundState[T](updatedState, roundHash)

      // figure out what the majority of bundles agreed upon
      val bundles = roundState.proposals

      // take those transactions bundle and sign them
      // TODO: temp logic
      val bundleProposal = bundles(updatedState.selfId.get)

      val bundle = bundleProposal match {
        case CheckpointProposal(data) =>
          data
        case ConflictProposal(data) =>
          data
      }

      val replyTo = roundState.replyTo

      // call actor callback with accepted bundle
      if (replyTo.isDefined) {
        replyTo.get ! ConsensusRoundResult(bundle, roundHash)
      }

      // TODO: do we need to gossip this event also?

      updatedState = cleanupRoundStateCache(updatedState, roundHash)
    }

    updatedState
  }
}

class Consensus(keyPair: KeyPair, udpActor: ActorRef)(implicit timeout: Timeout) extends Actor with ActorLogging {

  implicit val sys: ActorSystem = context.system
  implicit val kp: KeyPair = keyPair

  def receive: Receive = consensus(ConsensusRoundState(selfId = Some(Id(keyPair.getPublic)), udpActor = Some(udpActor)))

  def consensus(consensusRoundState: ConsensusRoundState): Receive = {

    case InitializeConsensusRound(facilitators, roundHash, replyTo) =>
      context.become(consensus(initializeConsensusRound(consensusRoundState, facilitators, roundHash, replyTo)))

    case ConsensusVote(id, vote, roundHash) =>
      context.become(consensus(handlePeerVote(consensusRoundState, id, vote, roundHash)))

    case ConsensusProposal(id, bundle, roundHash) =>
      context.become(consensus(handlePeerProposedBundle(consensusRoundState, id, bundle, roundHash)))
  }

}