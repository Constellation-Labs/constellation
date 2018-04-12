package org.constellation.state

import akka.actor.{Actor, ActorLogging, ActorRef}
import org.constellation.consensus.Consensus.ProposedBlockUpdated
import org.constellation.primitives.{Block, Transaction}
import org.constellation.primitives.Chain.Chain
import org.constellation.state.ChainStateManager._
import org.constellation.state.MemPoolManager.RemoveConfirmedTransactions

import scala.collection.immutable.HashMap

object ChainStateManager {

  // Commands
  case class AddBlock(block: Block, replyTo: ActorRef)
  case class GetCurrentChainState()
  case class CreateBlockProposal(memPools: HashMap[ActorRef, Seq[Transaction]], round: Long, replyTo: ActorRef)

  // Events
  case class BlockAddedToChain(previousBlock: Block)
  case class CurrentChainStateUpdated(chain: Chain)
}

class ChainStateManager(memPoolManagerActor: ActorRef) extends Actor with ActorLogging {

  var chain: Chain = Chain()

  override def receive: Receive = {
    case AddBlock(block, replyTo) =>
      log.debug(s"received add block request $block")

      if (!chain.chain.contains(block)) {
        chain = Chain(chain.chain :+ block)
        log.debug(s"updated chain for $self = $chain")
        memPoolManagerActor ! RemoveConfirmedTransactions(block.transactions)
        replyTo ! BlockAddedToChain(block)
      }

    case GetCurrentChainState =>
      log.debug(s"received GetCurrentChainState request")
      sender() ! CurrentChainStateUpdated(chain)

    case CreateBlockProposal(memPools, round, replyTo) =>
      log.debug("Attempting to create block proposal")

      val transactions: Seq[Transaction] = memPools.foldLeft(Seq[Transaction]()) {
        (result, b) => {
          result.union(b._2).distinct
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
