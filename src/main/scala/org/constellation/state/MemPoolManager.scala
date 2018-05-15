package org.constellation.state

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.typesafe.scalalogging.Logger
import org.constellation.LevelDB
import org.constellation.consensus.Consensus.{GetMemPool, GetMemPoolResponse}
import org.constellation.primitives.Schema.{GetUTXO, TX}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.MemPoolManager.{AddTransaction, GetMemPoolDirect, RemoveConfirmedTransactions}

import scala.collection.mutable
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

    val response = GetMemPoolResponse(memPoolProposal, round)
    if (memPoolProposal.nonEmpty) {
      println(s"MemPoolProposalNonEmpty ${memPoolProposal.size}")
    }

    replyTo ! response
  }

  def handleRemoveConfirmedTransactions(transactions: Seq[Transaction], memPool: ListBuffer[Transaction]): Unit = {
    transactions.foreach(t => {
      memPool.-=(t)
    })
  }

  case object GetMemPoolDirect

}

class MemPoolManager(db: LevelDB = null) extends Actor with ActorLogging {

  @volatile var memPool: ListBuffer[Transaction] = new ListBuffer[Transaction]
  val logger = Logger(s"MemPoolManager")

  // TODO: pull from config
  var memPoolProposalLimit = 20

  @volatile var memPoolTX: Set[TX] = Set()


  override def receive: Receive = {

    case tx: TX =>
      memPoolTX = memPoolTX ++ Set(tx)

    case b: Block =>
      logger.debug(s"Received block ${b.short}, removing ${b.transactions.length} txs from mempool of size ${memPool.size}")
      b.transactions.foreach{memPool.-=}
      logger.debug(s"Received block ${b.short}, removed ${b.transactions.length} txs - new mempool size ${memPool.size}")

    case GetMemPoolDirect =>
      sender() ! memPool

    case AddTransaction(transaction) =>
      //   log.debug(s"received add transaction request $transaction")

      MemPoolManager.handleAddTransaction(memPool, transaction)
      if (memPool.nonEmpty) {
        logger.debug(s"Added transaction ${transaction.short} - mem pool size: ${memPool.size}")
      }

    //

    case GetMemPool(replyTo, round) =>
      logger.debug(s"received get mem pool request $replyTo, $round, memPool Size: ${memPool.size}")

      MemPoolManager.handleGetMemPool(memPool, replyTo, round, memPoolProposalLimit)

    case RemoveConfirmedTransactions(transactions) =>
      //   log.debug(s"received remove confirmed transactions request $transactions")
      MemPoolManager.handleRemoveConfirmedTransactions(transactions, memPool)
  }

}
