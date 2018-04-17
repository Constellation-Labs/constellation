package org.constellation.state

import akka.actor.{Actor, ActorLogging, ActorRef}
import org.constellation.consensus.Consensus.{GetMemPool, MemPoolUpdated}
import org.constellation.primitives.Transaction
import org.constellation.state.MemPoolManager.{AddTransaction, RemoveConfirmedTransactions}

import scala.collection.mutable.ListBuffer

object MemPoolManager {

  // Commands
  case class AddTransaction(transaction: Transaction)

  case class RemoveConfirmedTransactions(transactions: Seq[Transaction])

  // Events

  def handleAddTransaction(memPool: ListBuffer[Transaction], transaction: Transaction): ListBuffer[Transaction] = {
    memPool.+=(transaction)
  }

  def handleGetMemPool(memPool: ListBuffer[Transaction], replyTo: ActorRef, round: Long, memPoolProposalLimit: Int): Unit = {
     // TODO: use dealer key to encrypt

    val memPoolProposal: Seq[Transaction] = memPool.take(memPoolProposalLimit)

    replyTo ! MemPoolUpdated(memPoolProposal, round)
  }

  def handleRemoveConfirmedTransactions(transactions: Seq[Transaction], memPool: ListBuffer[Transaction]): Unit = {
    transactions.foreach(t => {
      memPool.-=(t)
    })
  }

}

class MemPoolManager extends Actor with ActorLogging {

  var memPool: ListBuffer[Transaction] = new ListBuffer[Transaction]

  // TODO: pull from config
  var memPoolProposalLimit = 20

  override def receive: Receive = {
    case AddTransaction(transaction) =>
      log.debug(s"received add transaction request $transaction")

      MemPoolManager.handleAddTransaction(memPool, transaction)

    case GetMemPool(replyTo, round) =>
      log.debug(s"received get mem pool request $replyTo, $round")

      MemPoolManager.handleGetMemPool(memPool, replyTo, round, memPoolProposalLimit)

    case RemoveConfirmedTransactions(transactions) =>
      log.debug(s"received remove confirmed transactions request $transactions")
      MemPoolManager.handleRemoveConfirmedTransactions(transactions, memPool)
  }

}
