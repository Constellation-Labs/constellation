package org.constellation.state

import akka.actor.{Actor, ActorLogging}
import org.constellation.primitives.Block.Block
import org.constellation.state.ChainStateManager.AddBlock

import scala.collection.mutable.ListBuffer

object ChainStateManager {

  // Commands
  case class AddBlock(block: Block)

  // Events
}

class ChainStateManager extends Actor with ActorLogging {

  var chain: ListBuffer[Block] = new ListBuffer[Block]

  // TODO: add genesis block from config

  override def receive: Receive = {
    case AddBlock(block) =>
    log.debug(s"received add block request $block")
    chain.+=(block)

  }

}
