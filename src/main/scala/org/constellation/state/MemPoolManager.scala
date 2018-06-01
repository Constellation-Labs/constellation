package org.constellation.state

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.typesafe.scalalogging.Logger
import org.constellation.LevelDB
import org.constellation.primitives.Schema.{Bundle, BundleData, GetUTXO, TX}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.MemPoolManager.{AddTransaction, RemoveConfirmedTransactions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Try}

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

  def handleRemoveConfirmedTransactions(transactions: Seq[Transaction], memPool: Seq[Transaction]): Seq[Transaction] = {
    var memPoolUpdated = memPool

    transactions.foreach(t => {
      memPoolUpdated = memPoolUpdated.diff(Seq(t))
    })

    memPoolUpdated
  }

}

class MemPoolManager(db: LevelDB = null, heartbeatEnabled: Boolean = false) extends Actor with ActorLogging {

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

    case RemoveConfirmedTransactions(transactions) =>
      memPool = MemPoolManager.handleRemoveConfirmedTransactions(transactions, memPool)
  }

}
