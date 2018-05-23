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
                                   roundHash: String,
                                   gossipHistory: Seq[Gossip[ProductHash]],
                                   replyTo: ActorRef)

  // Events
  case class PeerVote(id: Id, vote: Vote, roundHash: String)
  case class PeerProposedBundle(id: Id, bundle: Bundle, roundHash: String)

  case class ConsensusRoundResult(bundle: Bundle, roundHash: String)

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
                                              udpActor: ActorRef)(implicit system: ActorSystem): Boolean = {

    // TODO: here replace with call to gossip actor

    notifyFacilitators(facilitators, self, (f) => {
      udpActor.udpSendToId(None, f)
    })
  }

  def performConsensusRound(consensusRoundState: ConsensusRoundState,
                            vote: Vote,
                            gossipHistory: Seq[Gossip[ProductHash]],
                            facilitators: Set[Id],
                            self: ActorRef,
                            udpAddress: InetSocketAddress,
                            replyTo: ActorRef): ConsensusRoundState = {


    // perform local vote, store it in the hash of transactions

    // tell everyone to perform a vote given the options

    // have event receiver of getting peer votes

    // once threshold of peer votes, choose a final proposal based on all of the proposals

    // send out final proposal, once we get enough in agreement return proposal back to replyTo

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

  def handlePeerVote(consensusRoundState: ConsensusRoundState,
                     peer: Id,
                     transactions: Seq[Transaction],
                     chainStateManager: ActorRef,
                     replyTo: ActorRef): ConsensusRoundState = {

    // TODO: round
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

  def handlePeerProposedBundle(consensusRoundState: ConsensusRoundState,
                               replyTo: ActorRef,
                               block: Block,
                               peer: Id): ConsensusRoundState = {

    val peerBlockProposals =
      consensusRoundState.peerBlockProposals +
        (block.round -> (consensusRoundState.peerBlockProposals.getOrElse(block.round, HashMap()) + (peer -> block)))

    val updatedState = consensusRoundState.copy(peerBlockProposals = peerBlockProposals)

    replyTo ! CheckConsensusResult(block.round)

    updatedState
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

    case PerformConsensusRound(facilitators, vote, roundHash, gossipHistory, replyTo) =>
      logger.debug(s"$selfId init consensus round with facilitators = $facilitators and vote = $vote")

      consensusRoundState =
        performConsensusRound(consensusRoundState, vote, gossipHistory, facilitators, self, udpAddress, replyTo)

    case PeerVote(id, vote, roundHash) =>
      consensusRoundState = handlePeerVote()

    case PeerProposedBundle(id, bundle, roundHash) =>
      consensusRoundState = handlePeerProposedBundle()

  }

}