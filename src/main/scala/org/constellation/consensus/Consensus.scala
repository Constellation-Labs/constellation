package org.constellation.consensus

import java.security.KeyPair

import akka.actor.{Actor, ActorLogging, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import constellation._
import org.constellation.DAO
import org.constellation.consensus.Consensus._
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, GetPeerInfo, PeerData}
import org.constellation.serializer.KryoSerializer

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext

// Unused currently -- bug causing issue, EdgeProcessor has a simpler version of this in
// formCheckpoint without facilitator crosstalk or set union stage

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

  case class RoundHash[+T <: CC](hash: String)

  trait RemoteMessage

  val gossipedVotes: TrieMap[String, Set[Id]] = TrieMap()
  val gossipedProposals: TrieMap[String, Set[Id]] = TrieMap()

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
                                 dao: DAO,
                                 roundStates: HashMap[RoundHash[_ <: CC], RoundState] = HashMap())

  def notifyFacilitatorsOfMessage(facilitators: Set[Id],
                                  self: Id,
                                  dao: DAO,
                                  message: RemoteMessage,
                                  path: String)(implicit system: ActorSystem): Boolean = {

    dao.peerManager ! APIBroadcast(func = _.post(path, KryoSerializer.serialize(message)), peerSubset = facilitators)

    true
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

  def peerThresholdMet[T <: CC](consensusRoundState: ConsensusRoundState, roundHash: RoundHash[T], facilitators: Set[Id])
                      (r: RoundState => HashMap[Id, _]): Boolean = {

    val selfId = consensusRoundState.dao.id.b58

    val roundState = getCurrentRoundState(consensusRoundState, roundHash)

    // TODO: update here to require a threshold, not every facilitator
    val facilitatorsMissingInfo = facilitators.filter(f => !r(roundState).contains(f))

    val roundStates = r(roundState).keySet

    facilitatorsMissingInfo.isEmpty && facilitators.nonEmpty
  }

  // TODO: here is where we call out to bundling logic
  def getConsensusBundle[T <: CC](consensusRoundState: ConsensusRoundState,
                                  roundHash: RoundHash[T])(implicit keyPair: KeyPair): CheckpointProposal = {
    val roundState = getCurrentRoundState(consensusRoundState, roundHash)

    // figure out what the majority of bundles agreed upon
    val bundles: HashMap[Id, _ <: ProposalData[_ <: CC]] = roundState.proposals

    val dao = consensusRoundState.dao

    // TODO temp
    val bundleProposal = bundles.toSeq.sortBy(_._1.b58).map(_._2).head.asInstanceOf[CheckpointProposal]

    bundleProposal
  }

  def getFacilitators(dao: DAO)(implicit timeout: Timeout): Set[Id] = {
    val peers = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().keySet
    peers
  }

  def handlePeerVote[T <: CC](consensusRoundState: ConsensusRoundState,
                     peer: Id,
                     vote: VoteData[T],
                     roundHash: RoundHash[T])(implicit system: ActorSystem, keyPair: KeyPair, timeout: Timeout): ConsensusRoundState = {

    var updatedState = updateRoundCache(consensusRoundState, peer, roundHash, vote)

    val roundState = getCurrentRoundState[T](updatedState, roundHash)

    val selfId = updatedState.selfId.get

    // TODO: update to use proper facilitator filtering
    val facilitators = getFacilitators(consensusRoundState.dao)

    // TODO
    // check signature to see if we have already signed this before or some max depth,
    // if not then broadcast
    if (!gossipedVotes.contains(roundHash.hash) || !gossipedVotes.get(roundHash.hash).contains(peer)) {
      val votes = gossipedVotes.getOrElseUpdate(roundHash.hash, Set())
      gossipedVotes(roundHash.hash) = votes.+(peer)

      notifyFacilitatorsOfMessage(facilitators, selfId, consensusRoundState.dao,
        ConsensusVote(peer, vote, roundHash), "checkpointEdgeVote")
    }

    if (peerThresholdMet(updatedState, roundHash, facilitators.+(selfId))(_.votes)) {
      // take those transactions bundle and sign them

      // TODO: here is where we take votes and create a bundle proposal

      // create a bundle proposal
      // figure out what the majority of votes agreed upon
      val votes = roundState.votes

      // TODO: temp logic
      val vote = votes.toSeq.sortBy(f => f._1.b58).map(f => f._2).head

      val proposal = vote match {
        case CheckpointVote(data) =>
          CheckpointProposal(data)
        case ConflictVote(data) =>
          // TODO
          //ConflictProposal(Bundle(BundleData(Seq()).signed()(keyPair = keyPair)))
          null
      }

      // TODO
      println(s"notify everyone of a consensus proposal")

      val proposalMessage = ConsensusProposal(selfId, proposal, roundHash)

      consensusRoundState.dao.consensus ! proposalMessage

      notifyFacilitatorsOfMessage(facilitators,
        selfId, consensusRoundState.dao, proposalMessage, "checkpointEdgeProposal")
    }

    updatedState
  }

  def handlePeerProposedBundle[T <: CC](consensusRoundState: ConsensusRoundState,
                               peer: Id,
                               proposal: ProposalData[T],
                               roundHash: RoundHash[T])(implicit keyPair: KeyPair, system: ActorSystem, timeout: Timeout): ConsensusRoundState = {

    var updatedState = updateRoundCache(consensusRoundState, peer, roundHash, proposal)

    // TODO: update to use proper facilitator filtering
    val facilitators = getFacilitators(consensusRoundState.dao)

    val selfId = updatedState.selfId.get

    if (!gossipedProposals.contains(roundHash.hash) || !gossipedProposals.get(roundHash.hash).contains(peer)) {
      val proposals = gossipedProposals.getOrElseUpdate(roundHash.hash, Set())

      gossipedVotes(roundHash.hash) = proposals.+(peer)

      notifyFacilitatorsOfMessage(facilitators,
        selfId, consensusRoundState.dao, ConsensusProposal(peer, proposal, roundHash), "checkpointEdgeProposal")
    }

    if (peerThresholdMet(updatedState, roundHash, facilitators.+(selfId))(_.proposals)) {
      // get the consensus bundle
      val bundle = getConsensusBundle(updatedState, roundHash)

      // call callback with accepted checkpoint
      consensusRoundState.dao.edgeProcessor ! ConsensusRoundResult(bundle.data, roundHash)

      // TODO: do we need to gossip this event also?
    //  updatedState = cleanupRoundStateCache(updatedState, roundHash)
    } else {
      val thing = false
    }

    updatedState
  }
}

class Consensus(dao: DAO)
               (implicit timeout: Timeout, executionContext: ExecutionContext, system: ActorSystem, keyPair: KeyPair) extends Actor with ActorLogging {

  def receive: Receive = consensus(ConsensusRoundState(dao = dao,
    selfId = Some(Id(keyPair.getPublic.encoded))))

  def consensus(consensusRoundState: ConsensusRoundState): Receive = {

    case ConsensusVote(id, vote, roundHash) =>
      log.debug(s"consensus vote for roundHash = $roundHash, id = $id")
      context.become(consensus(handlePeerVote(consensusRoundState, id, vote, roundHash)))

    case ConsensusProposal(id, bundle, roundHash) =>
      log.debug(s"consensus proposal for roundHash = $roundHash, id = $id")
      context.become(consensus(handlePeerProposedBundle(consensusRoundState, id, bundle, roundHash)))

  }

}