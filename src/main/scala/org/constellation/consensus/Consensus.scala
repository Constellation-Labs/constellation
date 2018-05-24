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

  // TODO: do we need to gossip after recieving this message from an external node to the other nodes?
  case class StartConsensusRound(id: Id, vote: Vote)

  // Events
  case class PeerVote(id: Id, vote: Vote, roundHash: RoundHash)
  case class PeerProposedBundle(id: Id, bundle: Bundle, roundHash: RoundHash)
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

  def notifyFacilitatorsToStartConsensusRound(facilitators: Set[Id],
                                              self: Id,
                                              vote: Vote,
                                              udpActor: ActorRef)(implicit system: ActorSystem): Boolean = {

    // TODO: here replace with call to gossip actor

    notifyFacilitators(facilitators, self, (f) => {
      udpActor.udpSendToId(StartConsensusRound(self, vote), f)
    })
  }

  def notifyFacilitatorsOfVote(facilitators: Set[Id],
                               self: Id,
                               vote: Vote,
                               roundHash: RoundHash,
                               udpActor: ActorRef)(implicit system: ActorSystem): Boolean = {

    // TODO: here replace with call to gossip actor

    notifyFacilitators(facilitators, self, (f) => {
      udpActor.udpSendToId(PeerVote(self, vote, roundHash), f)
    })
  }

  def notifyFacilitatorsOfBundle(facilitators: Set[Id],
                                 self: Id,
                                 bundle: Bundle,
                                 roundHash: RoundHash,
                                 udpActor: ActorRef)(implicit system: ActorSystem): Boolean = {

    // TODO: here replace with call to gossip actor

    notifyFacilitators(facilitators, self, (f) => {
      udpActor.udpSendToId(PeerProposedBundle(self, bundle, roundHash), f)
    })
  }

  def performConsensusRound(consensusRoundState: ConsensusRoundState,
                            vote: Vote,
                            gossipHistory: Seq[Gossip[_ <: ProductHash]],
                            facilitators: Set[Id],
                            replyTo: ActorRef)(implicit system: ActorSystem): ConsensusRoundState = {

    // store our local vote
    val voteRoundHash = vote.vote.data.voteRoundHash

    val updatedVotes = consensusRoundState.roundStates.getOrElse(RoundHash(voteRoundHash),
      RoundState()).votes + (consensusRoundState.selfId.get -> vote)

    val updatedRoundState = RoundState(facilitators = facilitators, votes = updatedVotes, replyTo = Some(replyTo))

    val updatedRoundStates = consensusRoundState.roundStates +
      (RoundHash(voteRoundHash) -> consensusRoundState.roundStates.getOrElse(RoundHash(voteRoundHash), updatedRoundState))

    val updatedState = consensusRoundState.copy(roundStates = updatedRoundStates)

    // tell everyone to perform a vote given the options
    notifyFacilitatorsToStartConsensusRound(
      facilitators,
      consensusRoundState.selfId.get,
      vote,
      consensusRoundState.udpActor.get)

    updatedState
  }

  def handlePeerVote(consensusRoundState: ConsensusRoundState,
                     peer: Id,
                     vote: Vote,
                     roundHash: RoundHash)(implicit system: ActorSystem, keyPair: KeyPair): ConsensusRoundState = {

    val roundState = consensusRoundState.roundStates.getOrElse(roundHash, RoundState())

    val updatedVotes = roundState.votes + (peer -> vote)

    val updatedRoundState = roundState.copy(votes = updatedVotes)

    val updatedRoundStates = consensusRoundState.roundStates +
      (roundHash -> consensusRoundState.roundStates.getOrElse(roundHash, updatedRoundState))

    var updatedState = consensusRoundState.copy(roundStates = updatedRoundStates)

    // check if we have enough votes to make a bundle proposal
    val facilitatorsWithoutVotes = updatedState.roundStates.getOrElse(roundHash, RoundState()).facilitators.filter(f => {
      val votes = updatedState.roundStates.getOrElse(roundHash, RoundState()).votes
      !votes.contains(f)
    })

    // TODO: update here to require a threshold, not every facilitator
    if (facilitatorsWithoutVotes.isEmpty) {
      // create a bundle proposal

      // figure out what the majority of votes agreed upon
      val votes = updatedState.roundStates.getOrElse(roundHash, RoundState()).votes

      // take those transactions bundle and sign them
      val roundState = updatedState.roundStates.getOrElse(roundHash, RoundState())

      val facilitators = roundState.facilitators

      // TODO: temp logic
      val bundle = Bundle(BundleData(votes(consensusRoundState.selfId.get).vote.data.accept).signed())

      // cache bundle and gossip bundle info
      updatedState = handlePeerProposedBundle(consensusRoundState, consensusRoundState.selfId.get, bundle, roundHash)

      notifyFacilitatorsOfBundle(
        facilitators,
        consensusRoundState.selfId.get,
        bundle,
        roundHash,
        consensusRoundState.udpActor.get)
    }

    // TODO: we gossip each vote?

    updatedState
  }

  def addBundleToCache(consensusRoundState: ConsensusRoundState,
                       peer: Id,
                       bundle: Bundle,
                       roundHash: RoundHash): ConsensusRoundState = {
    val roundState = consensusRoundState.roundStates.getOrElse(roundHash, RoundState())

    val updatedBundles = roundState.bundles + (peer -> bundle)

    val updatedRoundState = roundState.copy(bundles = updatedBundles)

    val updatedRoundStates = consensusRoundState.roundStates +
      (roundHash -> consensusRoundState.roundStates.getOrElse(roundHash, updatedRoundState))

    consensusRoundState.copy(roundStates = updatedRoundStates)
  }

  def handlePeerProposedBundle(consensusRoundState: ConsensusRoundState,
                               peer: Id,
                               bundle: Bundle,
                               roundHash: RoundHash): ConsensusRoundState = {

    var updatedState = addBundleToCache(consensusRoundState, peer, bundle, roundHash)

    // TODO: check if we have enough bundles then call the callback
    // with the majority bundle, gossip that we have accepted it

    // check if we have enough votes to make a bundle decision
    val facilitatorsWithoutBundleProposals = updatedState.roundStates.getOrElse(roundHash, RoundState()).facilitators.filter(f => {
      val votes = updatedState.roundStates.getOrElse(roundHash, RoundState()).bundles
      !votes.contains(f)
    })

    // TODO: update here to require a threshold, not every facilitator
    if (facilitatorsWithoutBundleProposals.isEmpty) {
      // figure out what the majority of bundles agreed upon
      val bundles = updatedState.roundStates.getOrElse(roundHash, RoundState()).bundles

      // take those transactions bundle and sign them
      // TODO: temp logic
      val bundle = bundles.get(updatedState.selfId.get)

      val replyTo = updatedState.roundStates.getOrElse(roundHash, RoundState()).replyTo

      // call actor callback with accepted bundle
      if (replyTo.isDefined) {
        replyTo.get ! ConsensusRoundResult(bundle.get, roundHash)
      }

      // TODO: do we need to gossip this event also?

      // cleanup the cache
      val roundStates = updatedState.roundStates.-(roundHash)

      updatedState = updatedState.copy(roundStates = roundStates)
    }

    updatedState
  }

  case class RoundHash(hash: String)

  case class RoundState(facilitators: Set[Id] = Set(),
                        votes: HashMap[Id, Vote] = HashMap(),
                        bundles: HashMap[Id, Bundle] = HashMap(),
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
      logger.debug(s"$selfId init consensus round with facilitators = $facilitators and vote = $vote")

      consensusRoundState =
        performConsensusRound(consensusRoundState, vote, gossipHistory, facilitators, replyTo)

    case PeerVote(id, vote, roundHash) =>
      consensusRoundState = handlePeerVote(consensusRoundState, id, vote, roundHash)

    case PeerProposedBundle(id, bundle, roundHash) =>
      consensusRoundState = handlePeerProposedBundle(consensusRoundState, id, bundle, roundHash)

  }

}