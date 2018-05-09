package org.constellation.state

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.Consensus.{GetMemPool, GetMemPoolResponse}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.MemPoolManager.{AddTransaction, RemoveConfirmedTransactions}

import scala.collection.mutable.ListBuffer

object MemPoolManager {

  // Commands
  case class AddTransaction(transaction: Transaction)

  case class RemoveConfirmedTransactions(transactions: Seq[Transaction])

  // Events

  def handleAddTransaction(memPool: Seq[Transaction], transaction: Transaction): Seq[Transaction] = {
    var updatedMemPool = memPool

    if (!memPool.contains(transaction)) {
      updatedMemPool = memPool :+ transaction
    }

    updatedMemPool
  }

  def handleGetMemPool(memPool: Seq[Transaction], replyTo: ActorRef, round: Long, memPoolProposalLimit: Int): Unit = {
    // TODO: use dealer key to encrypt

    val memPoolProposal: Seq[Transaction] = memPool.take(memPoolProposalLimit)

    val response = GetMemPoolResponse(memPoolProposal, round)

    if (memPoolProposal.nonEmpty) {
      println(s"MemPoolProposalNonEmpty ${memPoolProposal.size}")
    }

    replyTo ! response
  }

  def handleRemoveConfirmedTransactions(transactions: Seq[Transaction], memPool: Seq[Transaction]): Seq[Transaction] = {
    var memPoolUpdated = memPool

    transactions.foreach(t => {
      memPoolUpdated = memPoolUpdated.diff(Seq(t))
    })

    memPoolUpdated
  }

}

class MemPoolManager extends Actor with ActorLogging {

  @volatile var memPool: Seq[Transaction] = Seq[Transaction]()

  val logger = Logger(s"MemPoolManager")

  // TODO: pull from config
  var memPoolProposalLimit = 20

  override def receive: Receive = {

    case AddTransaction(transaction) =>
      memPool = MemPoolManager.handleAddTransaction(memPool, transaction)

      if (memPool.nonEmpty) {
        logger.debug(s"Added transaction ${transaction.short} - mem pool size: ${memPool.size}")
      }

    case GetMemPool(replyTo, round) =>
      logger.debug(s"received get mem pool request $replyTo, $round, memPool Size: ${memPool.size}")

      MemPoolManager.handleGetMemPool(memPool, replyTo, round, memPoolProposalLimit)

    case RemoveConfirmedTransactions(transactions) =>
      memPool = MemPoolManager.handleRemoveConfirmedTransactions(transactions, memPool)
  }

}
