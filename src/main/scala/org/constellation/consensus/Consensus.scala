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

  // Commands
  case class PerformConsensusRound(facilitators: Set[Id],
                                   vote: Vote,
                                   gossipHistory: Seq[Gossip[_ <: ProductHash]],
                                   replyTo: ActorRef)

  // TODO: do we need to gossip after receiving this message from an external node to the other nodes?
  case class StartConsensusRound(id: Id, vote: Vote) extends RemoteMessage

  // Events
  trait RemoteMessage
  case class PeerVote(id: Id, vote: Vote, roundHash: RoundHash) extends RemoteMessage
  case class PeerProposedBundle(id: Id, bundle: Bundle, roundHash: RoundHash) extends RemoteMessage
  case class ConsensusRoundResult(bundle: Bundle, roundHash: RoundHash)

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

  def notifyFacilitatorsOfVote(facilitators: Set[Id],
                               self: Id,
                               vote: Vote,
                               roundHash: RoundHash,
                               udpActor: ActorRef)(implicit system: ActorSystem): Boolean = {


    notifyFacilitators(facilitators, self, (f) => {
      udpActor.udpSendToId(PeerVote(self, vote, roundHash), f)
    })
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

  def performConsensusRound(consensusRoundState: ConsensusRoundState,
                            vote: Vote,
                            gossipHistory: Seq[Gossip[_ <: ProductHash]],
                            facilitators: Set[Id],
                            replyTo: ActorRef)(implicit system: ActorSystem): ConsensusRoundState = {

    val self = consensusRoundState.selfId.get
    val udpActor = consensusRoundState.udpActor.get

    // store our local vote
    val roundHash = RoundHash(vote.vote.data.voteRoundHash)

    var updatedState = updateVoteCache(consensusRoundState, self, roundHash, vote)

    val updatedRoundStates = updatedState.roundStates +
      (roundHash -> getCurrentRoundState(updatedState, roundHash).copy(facilitators = facilitators, replyTo = Some(replyTo)))

    updatedState = updatedState.copy(roundStates = updatedRoundStates)

    // tell everyone to perform a vote given the options
    notifyFacilitatorsOfMessage(facilitators, self, StartConsensusRound(self, vote), udpActor)

    updatedState
  }

  def getCurrentRoundState(consensusRoundState: ConsensusRoundState, roundHash: RoundHash): RoundState = {
    consensusRoundState.roundStates.getOrElse(roundHash, RoundState())
  }

  def updateRoundCache[T](consensusRoundState: ConsensusRoundState,
                          peer: Id,
                          roundHash: RoundHash,
                          event: T)
                         (r: RoundState => HashMap[Id, T])
                         (t: (RoundState, HashMap[Id, T]) => RoundState): ConsensusRoundState = {

    val roundState = getCurrentRoundState(consensusRoundState, roundHash)

    val updatedBundles = r(roundState) + (peer -> event)

    val updatedRoundState = t(roundState, updatedBundles)

    val updatedRoundStates = consensusRoundState.roundStates + (roundHash -> updatedRoundState)

    consensusRoundState.copy(roundStates = updatedRoundStates)
  }

  def updateVoteCache(consensusRoundState: ConsensusRoundState,
                      peer: Id,
                      roundHash: RoundHash,
                      vote: Vote): ConsensusRoundState = {
    updateRoundCache[Vote](consensusRoundState, peer, roundHash, vote)(_.votes)((t, r) => {
      t.votes = r
      t
    })
  }

  def updateBundleCache(consensusRoundState: ConsensusRoundState,
                        peer: Id,
                        roundHash: RoundHash,
                        bundle: Bundle): ConsensusRoundState = {
    updateRoundCache[Bundle](consensusRoundState, peer, roundHash, bundle)(_.bundles)((t, r) => {
      t.bundles = r
      t
    })
  }

  def cleanupRoundStateCache(consensusRoundState: ConsensusRoundState, roundHash: RoundHash): ConsensusRoundState = {
    val roundStates = consensusRoundState.roundStates.-(roundHash)
    consensusRoundState.copy(roundStates = roundStates)
  }

  def peerThresholdMet(consensusRoundState: ConsensusRoundState, roundHash: RoundHash)
                      (r: RoundState => HashMap[Id, _]): Boolean = {

    val roundState = getCurrentRoundState(consensusRoundState, roundHash)

    // TODO: update here to require a threshold, not every facilitator
    val facilitatorsMissingInfo = roundState.facilitators.filter(f => !r(roundState).contains(f))

    facilitatorsMissingInfo.isEmpty
  }

  def handlePeerVote(consensusRoundState: ConsensusRoundState,
                     peer: Id,
                     vote: Vote,
                     roundHash: RoundHash)(implicit system: ActorSystem, keyPair: KeyPair): ConsensusRoundState = {

    var updatedState = updateVoteCache(consensusRoundState, peer, roundHash, vote)

    if (peerThresholdMet(updatedState, roundHash)(_.votes)) {

      val roundState = getCurrentRoundState(updatedState, roundHash)

      // create a bundle proposal

      // figure out what the majority of votes agreed upon
      val votes = roundState.votes

      // take those transactions bundle and sign them
      val facilitators = roundState.facilitators

      // TODO: temp logic
      val vote = votes(consensusRoundState.selfId.get)
      val bundle = Bundle(BundleData(vote.vote.data.accept).signed()(keyPair = keyPair))

      // cache bundle and gossip bundle info
      updatedState =
        handlePeerProposedBundle(consensusRoundState, consensusRoundState.selfId.get, bundle, roundHash)

      val self = consensusRoundState.selfId.get
      val udpActor = consensusRoundState.udpActor.get

      notifyFacilitatorsOfMessage(facilitators,
        self, PeerProposedBundle(self, bundle, roundHash), udpActor)
    }

    // TODO: we gossip each vote?

    updatedState
  }

  def handlePeerProposedBundle(consensusRoundState: ConsensusRoundState,
                               peer: Id,
                               bundle: Bundle,
                               roundHash: RoundHash): ConsensusRoundState = {

    var updatedState = updateBundleCache(consensusRoundState, peer, roundHash, bundle)

    if (peerThresholdMet(updatedState, roundHash)(_.bundles)) {

      val roundState = getCurrentRoundState(updatedState, roundHash)

      // figure out what the majority of bundles agreed upon
      val bundles = roundState.bundles

      // take those transactions bundle and sign them
      // TODO: temp logic
      val bundle = bundles.get(updatedState.selfId.get)

      val replyTo = roundState.replyTo

      // call actor callback with accepted bundle
      if (replyTo.isDefined) {
        replyTo.get ! ConsensusRoundResult(bundle.get, roundHash)
      }

      // TODO: do we need to gossip this event also?

      updatedState = cleanupRoundStateCache(updatedState, roundHash)
    }

    updatedState
  }

  // Functions
  // Complete
  // 1. update cache with type of (peer -> bundle) or (peer -> vote) or (peer -> Seq(TX)) or (peer -> bundle)
  // 2. check if we meet the threshold of votes or proposals

  // TODO
  // 3. if we do, perform logic of creating vote or bundle
  // 4. optionally gossip information
  // 5. optionally clean up cache
  // 6. optionally call callback

  // roundHash: seq public keys + roundType(conflict, checkpoint)
  // votes: HashMap[Id, _ <: Event]
  // bundleProposals: HashMap[Id, Bundle]

  case class RoundHash(hash: String)

  case class RoundState(facilitators: Set[Id] = Set(),
                        var votes: HashMap[Id, Vote] = HashMap(),
                        var bundles: HashMap[Id, Bundle] = HashMap(),
                        replyTo: Option[ActorRef] = None)

  case class ConsensusRoundState(selfId: Option[Id] = None,
                                 udpActor: Option[ActorRef] = None,
                                 roundStates: HashMap[RoundHash, RoundState] = HashMap())

}

class Consensus(keyPair: KeyPair, udpActor: ActorRef)(implicit timeout: Timeout) extends Actor with ActorLogging {

  implicit val sys: ActorSystem = context.system
  implicit val kp: KeyPair = keyPair

  val logger = Logger(s"Consensus")

  private val selfId = Id(keyPair.getPublic)

  var consensusRoundState: ConsensusRoundState = ConsensusRoundState(selfId = Some(selfId), udpActor = Some(udpActor))

  override def receive: Receive = {

    case PerformConsensusRound(facilitators, vote, gossipHistory, replyTo) =>
      consensusRoundState = performConsensusRound(consensusRoundState, vote, gossipHistory, facilitators, replyTo)

    case PeerVote(id, vote, roundHash) =>
      consensusRoundState = handlePeerVote(consensusRoundState, id, vote, roundHash)

    case PeerProposedBundle(id, bundle, roundHash) =>
      consensusRoundState = handlePeerProposedBundle(consensusRoundState, id, bundle, roundHash)

  }

}