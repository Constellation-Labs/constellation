package org.constellation.state

import akka.actor.{Actor, ActorLogging, ActorRef}
import org.constellation.consensus.Consensus.ProposedBlockUpdated
import org.constellation.primitives.{Block, Transaction}
import org.constellation.primitives.Chain.Chain
import org.constellation.state.ChainStateManager._

import scala.collection.mutable

object ChainStateManager {

  // Commands
  case class AddBlock(block: Block)
  case class GetCurrentChainState()
  case class CreateBlockProposal(memPools: mutable.HashMap[ActorRef, Seq[Transaction]])

  // Events
  case class BlockAddedToChain(previousBlock: Block)
  case class CurrentChainStateUpdated(chain: Chain)
}

class ChainStateManager extends Actor with ActorLogging {

  var chain: Chain = Chain()

  override def receive: Receive = {
    case AddBlock(block) =>
      log.debug(s"received add block request $block")
      chain.chain += block
      sender() ! BlockAddedToChain(block)

    case GetCurrentChainState =>
      log.debug(s"received GetCurrentChainState request")
      sender() ! CurrentChainStateUpdated(chain)

    case CreateBlockProposal(memPools) =>
      log.debug("Attempting to create block proposal")

      val transactions: Seq[Transaction] = Seq()

      memPools.foreach(f => {
        transactions.union(f._2).distinct
      })

      val lastBlock = chain.chain.last

      // TODO: update to use proper sigs and participants
      val blockProposal: Block =  Block(lastBlock.signature, lastBlock.height + 1, "",
        lastBlock.clusterParticipants, lastBlock.round + 1, transactions)

      sender() ! ProposedBlockUpdated(blockProposal)
  }

}
