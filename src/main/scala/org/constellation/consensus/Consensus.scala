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
                                   gossipHistory: Seq[Gossip[ProductHash]],
                                   replyTo: ActorRef)

  case class StartConsensusRound(id: Id, vote: Vote)

  // Events
  case class PeerVote(id: Id, vote: Vote, roundHash: RoundHash)
  case class PeerProposedBundle(id: Id, bundle: Bundle, roundHash: RoundHash)

  case class ConsensusRoundResult(bundle: Bundle, roundHash: RoundHash)

  // Methods

  def isFacilitator(facilitators: Set[Id], self: Id): Boolean = {
    facilitators.contains(self)
  }

  def getConsensusBlock(peerBlockProposals: HashMap[Long, HashMap[Id, Block]],
                        currentFacilitators: Set[Id],
                        round: Long): Option[Block] = {

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

  def makeVote(vote: Vote, gossipHistory: Seq[Gossip[ProductHash]]): Vote ={
    // TODO: here do actual validation and voting

    vote
  }

  def performConsensusRound(consensusRoundState: ConsensusRoundState,
                            vote: Vote,
                            gossipHistory: Seq[Gossip[ProductHash]],
                            facilitators: Set[Id],
                            selfId: Id,
                            udpActor: ActorRef,
                            replyTo: ActorRef): ConsensusRoundState = {

    // store our local vote
    val voteRoundHash = vote.vote.data.voteRoundHash

    val updatedVotes = consensusRoundState.roundStates.getOrElse(RoundHash(voteRoundHash),
      RoundState()).votes + (consensusRoundState.selfId -> vote)

    val updatedRoundState = RoundState(facilitators = facilitators, votes = updatedVotes, replyTo = Some(replyTo))

    val updatedRoundStates = consensusRoundState.roundStates +
      (RoundHash(voteRoundHash) -> consensusRoundState.roundStates.getOrElse(RoundHash(voteRoundHash), updatedRoundState))

    val updatedState = consensusRoundState.copy(roundStates = updatedRoundStates)

    // tell everyone to perform a vote given the options
    notifyFacilitatorsToStartConsensusRound(facilitators, consensusRoundState.selfId, vote, udpActor)

    updatedState
  }

  def checkConsensusResult(consensusRoundState: ConsensusRoundState,
                           round: Long): Option[Block] = {

    /*

    val consensusBlock =
      Consensus.getConsensusBlock(consensusRoundState.peerBlockProposals,
        consensusRoundState.currentFacilitators,
        round)

    if (consensusBlock.isDefined && consensusRoundState.replyToWithConsensusResult.isDefined) {
      consensusRoundState.replyToWithConsensusResult.get ! ConsensusResult(consensusBlock.get, round)
    }

    consensusBlock
    */
  }

  def handlePeerVote(consensusRoundState: ConsensusRoundState,
                     peer: Id,
                     vote: Vote,
                     roundHash: RoundHash): ConsensusRoundState = {

    val updatedVotes = consensusRoundState.roundStates.getOrElse(roundHash,
      RoundState()).votes + (peer -> vote)

    val updatedRoundState = RoundState(votes = updatedVotes)

    val updatedRoundStates = consensusRoundState.roundStates +
      (roundHash -> consensusRoundState.roundStates.getOrElse(roundHash, updatedRoundState))

    val updatedState = consensusRoundState.copy(roundStates = updatedRoundStates)

    // check if we have enough votes to make a bundle proposal
    val facilitatorsWithoutVotes = updatedState.roundStates.getOrElse(roundHash, RoundState()).facilitators.filter(f => {
      val votes = updatedState.roundStates.getOrElse(roundHash, RoundState()).votes
      !votes.contains(f)
    })

    // TODO: update here to require a threshold, not every facilitator

    if (facilitatorsWithoutVotes.isEmpty) {
      // create a bundle proposal
    }

    updatedState
  }

  def handlePeerProposedBundle(consensusRoundState: ConsensusRoundState,
                               peer: Id,
                               bundle: Bundle,
                               roundHash: RoundHash): ConsensusRoundState = {

    /*
    val peerBlockProposals =
      consensusRoundState.peerBlockProposals +
        (block.round -> (consensusRoundState.peerBlockProposals.getOrElse(block.round, HashMap()) + (peer -> block)))

    val updatedState = consensusRoundState.copy(peerBlockProposals = peerBlockProposals)

    replyTo ! CheckConsensusResult(block.round)

    updatedState
    */
  }

  case class RoundHash(hash: String)

  case class RoundState(facilitators: Set[Id] = Set(),
                        votes: HashMap[Id, Vote] = HashMap(),
                        bundles: HashMap[Id, Bundle] = HashMap(),
                        replyTo: Option[ActorRef] = None)

  case class ConsensusRoundState(selfId: Id = null, roundStates: HashMap[RoundHash, RoundState] = HashMap())

}

class Consensus(keyPair: KeyPair,
                udpAddress: InetSocketAddress = new InetSocketAddress("127.0.0.1", 16180),
                udpActor: ActorRef)(implicit timeout: Timeout) extends Actor with ActorLogging {

  implicit val sys: ActorSystem = context.system

  val logger = Logger(s"Consensus")

  private val selfId = Id(keyPair.getPublic)

  @volatile var consensusRoundState: ConsensusRoundState = ConsensusRoundState(selfId = selfId)

  override def receive: Receive = {

    case PerformConsensusRound(facilitators, vote, gossipHistory, replyTo) =>
      logger.debug(s"$selfId init consensus round with facilitators = $facilitators and vote = $vote")

      consensusRoundState =
        performConsensusRound(consensusRoundState, vote, gossipHistory, facilitators, selfId, udpActor, replyTo)

    case PeerVote(id, vote, roundHash) =>
      consensusRoundState = handlePeerVote(consensusRoundState, id, vote, roundHash)

    case PeerProposedBundle(id, bundle, roundHash) =>
      consensusRoundState = handlePeerProposedBundle(consensusRoundState, id, bundle, roundHash)

  }

}