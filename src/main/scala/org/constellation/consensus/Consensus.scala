package org.constellation.consensus

import java.security.KeyPair

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.util.Timeout
import constellation._
import org.constellation.Data
import org.constellation.consensus.Consensus._
import org.constellation.consensus.EdgeProcessor.CreateCheckpointEdgeResponse
import org.constellation.p2p.UDPSend
import org.constellation.primitives.{APIBroadcast, PeerManager}
import org.constellation.primitives.Schema._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.Signed

import scala.collection.immutable.HashMap

object Consensus {
  sealed trait CC

  sealed trait Conflict extends CC
  sealed trait Checkpoint extends CC

  sealed trait CachedData[+T <: CC]

  sealed trait VoteData[+T <: CC] extends CachedData[T]
  sealed trait ProposalData[+T <: CC] extends CachedData[T]

  case class CheckpointVote(data: CheckpointBlock) extends VoteData[Checkpoint]
  case class ConflictVote(data: Vote) extends VoteData[Conflict]

  case class CheckpointProposal(data: CheckpointBlock) extends ProposalData[Checkpoint]
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

  case class ConsensusRoundResult[+T <: CC](checkpointBlock: CheckpointBlock, roundHash: RoundHash[T])

  case class RoundState(facilitators: Set[Id] = Set(),
                        votes: HashMap[Id, _ <: VoteData[_ <: CC]] = HashMap(),
                        proposals: HashMap[Id, _ <: ProposalData[_ <: CC]] = HashMap(),
                        callback: ConsensusRoundResult[_ <: CC] => Unit = (result: ConsensusRoundResult[_ <: CC]) => {})

  case class ConsensusRoundState(selfId: Option[Id] = None,
                                 dao: Data,
                                 peerManager: ActorRef,
                                 roundStates: HashMap[RoundHash[_ <: CC], RoundState] = HashMap())

  def notifyFacilitatorsOfMessage(facilitators: Set[Id],
                                  self: Id,
                                  dao: Data,
                                  message: RemoteMessage,
                                  peerManager: ActorRef,
                                  path: String)(implicit system: ActorSystem): Boolean = {

    peerManager ! APIBroadcast(func = _.post(path, KryoSerializer.serialize(message)), peerSubset = facilitators)

    true
  }

  def initializeConsensusRound[T <: CC](consensusRoundState: ConsensusRoundState,
                                        facilitators: Set[Id],
                                        roundHash: RoundHash[T],
                                        cb: ConsensusRoundResult[_ <: CC] => Unit,
                                        vote: VoteData[T])
                                       (implicit system: ActorSystem, keyPair: KeyPair): ConsensusRoundState = {

    // TODO check if already initialized if so don't broadcast again to start

    val self = consensusRoundState.selfId.get

    println(s"init $self consensus round hash: $roundHash, vote= $vote")

    val updatedRoundStates = consensusRoundState.roundStates +
      (roundHash -> getCurrentRoundState(consensusRoundState, roundHash)
        .copy(facilitators = facilitators, callback = cb))

    // update local cache with self vote
    val updatedState =
      handlePeerVote(consensusRoundState.copy(roundStates = updatedRoundStates), self, vote, roundHash)

    // tell everyone to perform a vote
    // TODO: update to only run during conflict voting, for now it's ignored
    notifyFacilitatorsOfMessage(facilitators, self, consensusRoundState.dao,
      StartConsensusRound(self, vote, roundHash), consensusRoundState.peerManager, "startConsensusRound")

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
  def getConsensusBundle[T <: CC](consensusRoundState: ConsensusRoundState,
                                  roundHash: RoundHash[T])(implicit keyPair: KeyPair): CheckpointProposal = {
    val roundState = getCurrentRoundState(consensusRoundState, roundHash)

    // figure out what the majority of bundles agreed upon
    val bundles = roundState.proposals

    val dao = consensusRoundState.dao

    // TODO temp
    val bundleProposal = bundles(consensusRoundState.selfId.get).asInstanceOf[CheckpointProposal]

    bundleProposal
  }

  def handlePeerVote[T <: CC](consensusRoundState: ConsensusRoundState,
                     peer: Id,
                     vote: VoteData[T],
                     roundHash: RoundHash[T])(implicit system: ActorSystem, keyPair: KeyPair): ConsensusRoundState = {

    var updatedState = updateRoundCache(consensusRoundState, peer, roundHash, vote)

    val roundState = getCurrentRoundState[T](updatedState, roundHash)

    val selfId = updatedState.selfId.get

    val facilitators = roundState.facilitators

    // TODO
    // check signature to see if we have already signed this before or some max depth,
    // if not then broadcast

    /*
    if (firstTimeObservingVote) {
      println(s"first time observing vote for person = $peer")

      notifyFacilitatorsOfMessage(facilitators, selfId, consensusRoundState.dao,
        StartConsensusRound(selfId, vote, roundHash), consensusRoundState.peerManager, "startConsensusRound")
    }
    */

    if (peerThresholdMet(updatedState, roundHash)(_.votes)) {
      // take those transactions bundle and sign them

      // TODO: here is where we take votes and create a bundle proposal

      // create a bundle proposal
      // figure out what the majority of votes agreed upon
      val votes = roundState.votes

      // TODO: temp logic
      val vote = votes(updatedState.selfId.get)

      val proposal = vote match {
        case CheckpointVote(data) =>
          CheckpointProposal(data)
        case ConflictVote(data) =>
          // TODO
          ConflictProposal(Bundle(BundleData(Seq()).signed()(keyPair = keyPair)))
      }

      updatedState =
          handlePeerProposedBundle(updatedState, selfId, proposal, roundHash)

      // TODO
      println(s"notify everyone of a consensus proposal")

      notifyFacilitatorsOfMessage(facilitators,
        selfId, consensusRoundState.dao, ConsensusProposal(selfId, proposal, roundHash), consensusRoundState.peerManager, "checkpointEdgeProposal")
    }

    updatedState
  }

  def handlePeerProposedBundle[T <: CC](consensusRoundState: ConsensusRoundState,
                               peer: Id,
                               proposal: ProposalData[T],
                               roundHash: RoundHash[T])(implicit keyPair: KeyPair, system: ActorSystem): ConsensusRoundState = {

    // TODO: make based on signature instead
    val firstTimeObservingProposal = !consensusRoundState.roundStates.contains(roundHash) || !consensusRoundState.roundStates(roundHash).proposals.contains(peer)

    var updatedState = updateRoundCache(consensusRoundState, peer, roundHash, proposal)

    val facilitators = updatedState.roundStates(roundHash).facilitators

    val selfId = updatedState.selfId.get

    if (firstTimeObservingProposal) {
      println(s"first time observing vote for proposal = $peer")

      /*
      notifyFacilitatorsOfMessage(facilitators,
        selfId, consensusRoundState.dao, ConsensusProposal(selfId, proposal, roundHash), consensusRoundState.peerManager, "checkpointEdgeProposal")
      */
    }

    if (peerThresholdMet(updatedState, roundHash)(_.proposals)) {
      val roundState = getCurrentRoundState(updatedState, roundHash)

      // get the consensus bundle
      val bundle = getConsensusBundle(updatedState, roundHash)

      // TODO: replace callback with message sent to edge processor
      // call callback with accepted bundle
      roundState.callback(ConsensusRoundResult(bundle.data, roundHash))

      // TODO: do we need to gossip this event also?
      updatedState = cleanupRoundStateCache(updatedState, roundHash)
    }

    updatedState
  }
}

class Consensus(keyPair: KeyPair, dao: Data, peerManager: ActorRef)
               (implicit timeout: Timeout) extends Actor with ActorLogging {

  implicit val sys: ActorSystem = context.system
  implicit val kp: KeyPair = keyPair

  def receive: Receive = consensus(ConsensusRoundState(dao = dao,
    selfId = Some(Id(keyPair.getPublic.encoded)), peerManager = peerManager))

  def consensus(consensusRoundState: ConsensusRoundState): Receive = {

    // TODO: remove
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