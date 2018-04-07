package org.constellation.state

import akka.actor.{Actor, ActorLogging}
import org.constellation.consensus.Consensus.{GetMemPool, MemPoolUpdated}
import org.constellation.primitives.Transaction
import org.constellation.state.MemPoolManager.{AddTransaction, RemoveConfirmedTransactions}

import scala.collection.mutable.ListBuffer

object MemPoolManager {

  // Commands
  case class AddTransaction(transaction: Transaction)

  case class RemoveConfirmedTransactions(transactions: Seq[Transaction])

  // Events
}

class MemPoolManager extends Actor with ActorLogging {

  var memPool: ListBuffer[Transaction] = new ListBuffer[Transaction]

  // TODO: pull from config
  var memPoolProposalLimit = 20

  override def receive: Receive = {
    case AddTransaction(transaction) =>
      log.debug(s"received add transaction request $transaction")

      memPool.+=(transaction)

    case GetMemPool(replyTo, round) =>
      // TODO: use dealer key to encrypt

      val memPoolProposal: Seq[Transaction] = memPool.take(memPoolProposalLimit)

      replyTo ! MemPoolUpdated(memPoolProposal, round)

    case RemoveConfirmedTransactions(transactions) =>
      transactions.foreach(t => {
        memPool.-=(t)
      })

  }

}
