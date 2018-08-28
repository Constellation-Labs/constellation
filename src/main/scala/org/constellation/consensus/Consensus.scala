package org.constellation.consensus

import java.security.KeyPair

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.util.Timeout
import constellation._
import org.constellation.Data
import org.constellation.consensus.Consensus._
import org.constellation.p2p.UDPSend
import org.constellation.primitives.EdgeService
import org.constellation.primitives.Schema._
import org.constellation.util.Signed

import scala.collection.immutable.HashMap

object Consensus {
  sealed trait CC

  sealed trait Conflict extends CC
  sealed trait Checkpoint extends CC

  sealed trait CachedData[+T <: CC]

  sealed trait VoteData[+T <: CC] extends CachedData[T]
  sealed trait ProposalData[+T <: CC] extends CachedData[T]

  case class CheckpointVote(data: CheckpointEdge) extends VoteData[Checkpoint]
  case class ConflictVote(data: Vote) extends VoteData[Conflict]

  case class CheckpointProposal(data: CheckpointEdge) extends ProposalData[Checkpoint]
  case class ConflictProposal(data: Bundle) extends ProposalData[Conflict]

  case class RoundHash[+T <: CC](hash: String)

  trait RemoteMessage

  case class ConsensusVote[+T <: CC](id: Id, data: VoteData[T], roundHash: RoundHash[T]) extends RemoteMessage
  case class ConsensusProposal[+T <: CC](id: Id, data: ProposalData[T], roundHash: RoundHash[T]) extends RemoteMessage
  case class StartConsensusRound[T <: CC](id: Id, data: VoteData[T], roundHash: RoundHash[T]) extends RemoteMessage

  case class InitializeConsensusRound[+T <: CC](facilitators: Set[Id],
                                                roundHash: RoundHash[T],
                                                callback: ConsensusRoundResult[_ <: CC] => Unit,
                                                vote: VoteData[T])

  case class ConsensusRoundResult[+T <: CC](bundle: CheckpointEdge, roundHash: RoundHash[T])

  case class RoundState(facilitators: Set[Id] = Set(),
                        votes: HashMap[Id, _ <: VoteData[_ <: CC]] = HashMap(),
                        proposals: HashMap[Id, _ <: ProposalData[_ <: CC]] = HashMap(),
                        callback: ConsensusRoundResult[_ <: CC] => Unit = (result: ConsensusRoundResult[_ <: CC]) => {})

  case class ConsensusRoundState(selfId: Option[Id] = None,
                                 dao: Data,
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
    //  udpActor ! UDPSend(message, peerIdLookup(f).data.externalAddress)
    })

    true
  }

  def initializeConsensusRound[T <: CC](consensusRoundState: ConsensusRoundState,
                                        facilitators: Set[Id],
                                        roundHash: RoundHash[T],
                                        cb: ConsensusRoundResult[_ <: CC] => Unit,
                                        vote: VoteData[T])
                                       (implicit system: ActorSystem, keyPair: KeyPair): ConsensusRoundState = {

    val self = consensusRoundState.selfId.get
    val udpActor = consensusRoundState.udpActor.get

    val updatedRoundStates = consensusRoundState.roundStates +
      (roundHash -> getCurrentRoundState(consensusRoundState, roundHash)
        .copy(facilitators = facilitators, callback = cb))

    // update local cache with self vote
    val updatedState =
      handlePeerVote(consensusRoundState.copy(roundStates = updatedRoundStates), self, vote, roundHash)

    // tell everyone to perform a vote
    // TODO: update to only run during conflict voting, for now it's ignored
    notifyFacilitatorsOfMessage(facilitators, self,
      StartConsensusRound(self, vote, roundHash), udpActor)

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

    facilitatorsMissingInfo.isEmpty && roundState.facilitators.nonEmpty
  }

  // TODO: here is where we call out to bundling logic
  def getConsensusBundle[T <: CC](consensusRoundState: ConsensusRoundState, roundHash: RoundHash[T])(implicit keyPair: KeyPair): CheckpointEdge = {
    val roundState = getCurrentRoundState(consensusRoundState, roundHash)
    // figure out what the majority of bundles agreed upon
    val bundles = roundState.proposals

    EdgeService.createCheckpointEdge(consensusRoundState.dao.validationTips, consensusRoundState.dao.memPool.toSeq)

    /*
    bundleProposal match {
      case CheckpointProposal(data) =>
        data
      case ConflictProposal(data) =>
        data
    }
    */
  }

  def handlePeerVote[T <: CC](consensusRoundState: ConsensusRoundState,
                     peer: Id,
                     vote: VoteData[T],
                     roundHash: RoundHash[T])(implicit system: ActorSystem, keyPair: KeyPair): ConsensusRoundState = {

    var updatedState = updateRoundCache(consensusRoundState, peer, roundHash, vote)

    val selfId = updatedState.selfId.get

    if (peerThresholdMet(updatedState, roundHash)(_.votes)) {
      val roundState = getCurrentRoundState[T](updatedState, roundHash)

      // take those transactions bundle and sign them
      val facilitators = roundState.facilitators

      // TODO: here is where we take votes and create a bundle proposal
      val self = updatedState.selfId.get
      val udpActor = updatedState.udpActor.get

      // create a bundle proposal
      // figure out what the majority of votes agreed upon
      val votes = roundState.votes

      // TODO: temp logic
      val vote = votes(updatedState.selfId.get)


      val proposal = vote match {
        case CheckpointVote(data) =>
          // TODO: sign
          CheckpointProposal(data)
        case ConflictVote(data) =>
          ConflictProposal(Bundle(BundleData(data.vote.data.accept).signed()(keyPair = keyPair)))
      }

      updatedState =
          handlePeerProposedBundle(updatedState, selfId, proposal, roundHash)

      notifyFacilitatorsOfMessage(facilitators,
        self, ConsensusProposal(self, proposal, roundHash), udpActor)
    }

    updatedState
  }

  def handlePeerProposedBundle[T <: CC](consensusRoundState: ConsensusRoundState,
                               peer: Id,
                               bundle: ProposalData[T],
                               roundHash: RoundHash[T])(implicit keyPair: KeyPair): ConsensusRoundState = {

    var updatedState = updateRoundCache(consensusRoundState, peer, roundHash, bundle)

    if (peerThresholdMet(updatedState, roundHash)(_.proposals)) {
      val roundState = getCurrentRoundState(updatedState, roundHash)

      // get the consensus bundle
      val bundle = getConsensusBundle(updatedState, roundHash)

      // call callback with accepted bundle
      roundState.callback(ConsensusRoundResult(bundle, roundHash))

      // TODO: do we need to gossip this event also?
      updatedState = cleanupRoundStateCache(updatedState, roundHash)
    }

    updatedState
  }
}

class Consensus(keyPair: KeyPair, dao: Data, udpActor: ActorRef)(implicit timeout: Timeout) extends Actor with ActorLogging {

  implicit val sys: ActorSystem = context.system
  implicit val kp: KeyPair = keyPair

  def receive: Receive = consensus(ConsensusRoundState(dao = dao, selfId = Some(Id(keyPair.getPublic.encoded)), udpActor = Some(udpActor)))

  def consensus(consensusRoundState: ConsensusRoundState): Receive = {

    case InitializeConsensusRound(facilitators, roundHash, callback, vote) =>
      log.debug(s"init consensus round message roundHash = $roundHash")
      context.become(consensus(initializeConsensusRound(consensusRoundState, facilitators, roundHash, callback, vote)))

    case ConsensusVote(id, vote, roundHash) =>
      log.debug(s"consensus vote for roundHash = $roundHash, id = $id")
      context.become(consensus(handlePeerVote(consensusRoundState, id, vote, roundHash)))

    case ConsensusProposal(id, bundle, roundHash) =>
      log.debug(s"consensus proposal for roundHash = $roundHash, id = $id")
      context.become(consensus(handlePeerProposedBundle(consensusRoundState, id, bundle, roundHash)))
  }

}