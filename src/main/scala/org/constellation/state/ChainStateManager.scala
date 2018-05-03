package org.constellation.state

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.Consensus.{PeerProposedBlock, ProposedBlockUpdated, RequestBlockProposal}
import org.constellation.p2p.PeerToPeer.Id
import org.constellation.p2p.{UDPSendToID, UDPSendToIDByte}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.primitives.Chain.Chain
import org.constellation.state.ChainStateManager._
import org.constellation.state.MemPoolManager.RemoveConfirmedTransactions

import scala.collection.immutable.HashMap
import scala.util.{Failure, Try}

object ChainStateManager {

  // Commands
  case class AddBlock(block: Block, replyTo: ActorRef)
  case class GetCurrentChainState()
  case class CreateBlockProposal(memPools: HashMap[Id, Seq[Transaction]], round: Long, replyTo: ActorRef)

  // Events
  case class BlockAddedToChain(previousBlock: Block)
  case class CurrentChainStateUpdated(chain: Chain)
  case class GetLastBlockProposal()

  def handleAddBlock(chain: Chain, block: Block, memPoolManager: ActorRef, replyTo: ActorRef): Chain = {
    var updatedChain = chain

    if (!updatedChain.chain.contains(block)) {
      updatedChain = Chain(updatedChain.chain :+ block)
      memPoolManager ! RemoveConfirmedTransactions(block.transactions)
      replyTo ! BlockAddedToChain(block)
    }

    updatedChain
  }

  def handleCreateBlockProposal(memPools: Map[Id, Seq[Transaction]], chain: Chain, round: Long, replyTo: ActorRef): Block = {
    val transactions: Seq[Transaction] = memPools.foldLeft(Seq[Transaction]()) {
      (result, b) => {
        result.union(b._2).distinct.sortBy(t => t.sequenceNum)
      }
    }

    val lastBlock = chain.chain.last

    val round = lastBlock.round + 1

    // TODO: update to use proper sigs and participants
    val blockProposal: Block =  Block(lastBlock.signature, lastBlock.height + 1, "",
      lastBlock.clusterParticipants, round, transactions)

    replyTo ! ProposedBlockUpdated(blockProposal)
    blockProposal
  }

  case object GetChain

}

class ChainStateManager(memPoolManagerActor: ActorRef, selfId: Id = null) extends Actor with ActorLogging {

  var chain: Chain = Chain()
  val logger = Logger(s"ChainStateManager")
  @volatile var lastBlockProposed: Option[Block] = None

  override def receive: Receive = {

    case GetChain => sender() ! chain

    case RequestBlockProposal(round, id) =>

      logger.debug(s"RequestBlockProposal round: $round send to: ${id.short}")

      Try {
        if (lastBlockProposed.nonEmpty) {
          sender() ! UDPSendToID(PeerProposedBlock(lastBlockProposed.get, selfId), id)
        } else {
          val transactions: Seq[Transaction] = Seq()
          val lastBlock = chain.chain.last
          val round = lastBlock.round + 1
          // TODO: update to use proper sigs and participants
          val blockProposal: Block = Block(lastBlock.signature, lastBlock.height + 1, "",
            lastBlock.clusterParticipants, round, transactions)
          lastBlockProposed = Some(blockProposal)
          sender() ! UDPSendToID(PeerProposedBlock(lastBlockProposed.get, selfId), id)
        }
      } match {
        case Failure(e) => e.printStackTrace()
        case _ =>
      }

    case AddBlock(block, replyTo) =>

      if (block.transactions.nonEmpty) {
       // logger.debug(s"received add block request $block")
      }

      chain = handleAddBlock(chain, block, memPoolManagerActor, replyTo)

    case GetCurrentChainState =>
      //   log.debug(s"received GetCurrentChainState request")
      sender() ! CurrentChainStateUpdated(chain)

    case CreateBlockProposal(memPools, round, replyTo) =>
       //  logger.debug("Attempting to create block proposal")

      lastBlockProposed = Some(handleCreateBlockProposal(memPools, chain, round, replyTo))
/*
    case GetLastBlockProposal =>
      if (lastBlockProposed.nonEmpty) {
        sender() ! lastBlockProposed
      } else {
        val transactions: Seq[Transaction] = Seq()
        val lastBlock = chain.chain.last
        val round = lastBlock.round + 1
        // TODO: update to use proper sigs and participants
        val blockProposal: Block =  Block(lastBlock.signature, lastBlock.height + 1, "",
          lastBlock.clusterParticipants, round, transactions)
        lastBlockProposed = Some(blockProposal)
        sender() ! lastBlockProposed
      }*/
  }

}

