package org.constellation.state

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef}
import org.constellation.consensus.Consensus.ProposedBlockUpdated
import org.constellation.p2p.PeerToPeer.Id
import org.constellation.primitives.{Block, Transaction}
import org.constellation.primitives.Chain.Chain
import org.constellation.state.ChainStateManager._
import org.constellation.state.MemPoolManager.RemoveConfirmedTransactions

import scala.collection.immutable.HashMap

object ChainStateManager {

  // Commands
  case class AddBlock(block: Block, replyTo: ActorRef)
  case class GetCurrentChainState()
  case class CreateBlockProposal(memPools: HashMap[Id, Seq[Transaction]], round: Long, replyTo: ActorRef)

  // Events
  case class BlockAddedToChain(previousBlock: Block)
  case class CurrentChainStateUpdated(chain: Chain)

  def handleAddBlock(chain: Chain, block: Block, memPoolManager: ActorRef, replyTo: ActorRef): Chain = {
    var updatedChain = chain

    if (!updatedChain.chain.contains(block)) {
      updatedChain = Chain(updatedChain.chain :+ block)
      memPoolManager ! RemoveConfirmedTransactions(block.transactions)
      replyTo ! BlockAddedToChain(block)
    }

    updatedChain
  }

  def handleCreateBlockProposal(memPools: Map[Id, Seq[Transaction]], chain: Chain, round: Long, replyTo: ActorRef): Unit = {
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
  }

}

class ChainStateManager(memPoolManagerActor: ActorRef) extends Actor with ActorLogging {

  var chain: Chain = Chain()

  override def receive: Receive = {
    case AddBlock(block, replyTo) =>
      log.debug(s"received add block request $block, $replyTo")

      chain = handleAddBlock(chain, block, memPoolManagerActor, replyTo)

    case GetCurrentChainState =>
      log.debug(s"received GetCurrentChainState request")
      sender() ! CurrentChainStateUpdated(chain)

    case CreateBlockProposal(memPools, round, replyTo) =>
      log.debug("Attempting to create block proposal")

      handleCreateBlockProposal(memPools, chain, round, replyTo)
  }

}
