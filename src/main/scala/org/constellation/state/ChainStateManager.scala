package org.constellation.state

import akka.actor.{Actor, ActorLogging}
import org.constellation.primitives.Block
import org.constellation.primitives.Chain.Chain
import org.constellation.state.ChainStateManager.{AddBlock, CurrentChainStateUpdated, GetCurrentChainState}

object ChainStateManager {

  // Commands
  case class AddBlock(block: Block)
  case class GetCurrentChainState()

  // Events
  case class CurrentChainStateUpdated(chain: Chain)

}

class ChainStateManager extends Actor with ActorLogging {

  var chain: Chain = Chain()

  // TODO: add genesis block from config

  override def receive: Receive = {
    case AddBlock(block) =>
      log.debug(s"received add block request $block")
      chain.chain += block

    case GetCurrentChainState =>
      log.debug(s"received GetCurrentChainState request")
      sender() ! CurrentChainStateUpdated(chain)
  }

}
