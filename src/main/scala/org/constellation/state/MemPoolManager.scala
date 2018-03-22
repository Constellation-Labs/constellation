package org.constellation.state

import akka.actor.{Actor, ActorLogging}
import org.constellation.consensus.Consensus.GetProposedBlock
import org.constellation.primitives.Transaction
import org.constellation.state.MemPoolManager.AddTransaction

import scala.collection.mutable.ListBuffer

object MemPoolManager {

  // Commands
  case class AddTransaction(transaction: Transaction)

  // Events
}

class MemPoolManager extends Actor with ActorLogging {

  var memPool: ListBuffer[Transaction] = new ListBuffer[Transaction]

  override def receive: Receive = {
    case AddTransaction(transaction) =>
      log.debug(s"received add transaction request $transaction")

      memPool.+=(transaction)

    case GetProposedBlock(dealerPublicKey) =>
      log.debug(s"received get proposed block request $dealerPublicKey")
      // TODO: call method to take from memPool, use deal key to sign, send to caller
  }

}
