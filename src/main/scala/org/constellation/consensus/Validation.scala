package org.constellation.consensus

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import org.constellation.consensus.EdgeProcessor.signFlow
import org.constellation.primitives.Schema.{AddressCacheData, CheckpointBlock, Transaction, TransactionCacheData}
import org.constellation.{DAO, KVDB}

import scala.concurrent.ExecutionContext

object Validation {
  /**
    *
    * @param transactions
    * @param cb
    * @return
    */
  def validByTransactionAncestors(transactions: Seq[TransactionValidationStatus], cb: CheckpointBlock): Boolean =
    transactions.nonEmpty && transactions.forall { s: TransactionValidationStatus =>
      cb.checkpoint.edge.parentHashes.forall { ancestorHash =>
        s.validByAncestor(ancestorHash)
      }
    }

  /**
    *
    * @param dao
    * @param cb
    * @param executionContext
    * @return
    */
  def validateCheckpointBlock(dao: DAO, cb: CheckpointBlock)(implicit executionContext: ExecutionContext): CheckpointValidationStatus = {
    val transactions: Seq[TransactionValidationStatus] = cb.transactions.map { tx => Validation.validateTransaction(dao.dbActor, tx) }
    val validatedByTransactions = transactions.forall(_.validByCurrentState)
    val validByAncestors = validByTransactionAncestors(transactions, cb)
    val validByBatch = transactions.groupBy(_.transaction.src).filter { case (src, txs) =>
      val batchTotal = txs.map(_.transaction.amount).sum
      val currentBalance = 0 // Todo, check according to a ledger state and mempool if double spend, keep track of double spend votes and report back to conflict resolution
      currentBalance + batchTotal >= 0
    }
    val doubleSpentTransactions = validByBatch.values
    val isValid = validatedByTransactions && validByAncestors && doubleSpentTransactions.isEmpty
    if (isValid) signFlow(dao, cb)
    CheckpointValidationStatus(cb, isValid, doubleSpentTransactions)
  }

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  // TODO : Add an LRU cache for looking up TransactionCacheData instead of pure LDB calls.

  case class CheckpointValidationStatus(cp: CheckpointBlock,
                                        isValid: Boolean,
                                        invalidTxs: Iterable[Seq[TransactionValidationStatus]]
                                        )

  case class TransactionValidationStatus(
                                        transaction: Transaction,
                                        transactionCacheData: Option[TransactionCacheData],
                                        addressCacheData: Option[AddressCacheData]
                                        ) {
    def isDuplicateHash: Boolean = transactionCacheData.exists{_.inDAG}
    def sufficientBalance: Boolean = addressCacheData.exists{c =>
      c.balance >= transaction.amount
    }
    def sufficientMemPoolBalance: Boolean = addressCacheData.exists{c =>
      c.memPoolBalance >= transaction.amount
    }

    def validByCurrentState: Boolean = !isDuplicateHash && sufficientBalance
    def validByCurrentStateMemPool: Boolean = !isDuplicateHash && sufficientBalance && sufficientMemPoolBalance
    def validByAncestor(ancestorHash: String): Boolean = {
      transactionCacheData.exists{t => !t.inDAGByAncestor.contains(ancestorHash)} &&
      addressCacheData.exists(_.ancestorBalances.get(ancestorHash).exists(_ >= transaction.amount))
    }
    // Need separate validator here for CB validation vs. mempool addition
    // I.e. don't check mempool balance when validating a CB because it takes precedence over
    // a new TX which is being added to mempool and conflicts with current mempool values.
  }

  /**
    * Check if a transaction already exists on chain or if there's a sufficient balance to process it
    * TODO: Use a bloom filter
    * @param dbActor: Reference to LevelDB DAO
    * @param tx : Resolved transaction
    * @return Future of whether or not the transaction should be considered valid
    * **/
  def validateTransaction(dbActor: KVDB, tx: Transaction): TransactionValidationStatus = {

    // A transaction should only be considered in the DAG once it has been committed to a checkpoint block.
    // Before that, it exists only in the memPool and is not stored in the database.
    val txCache = dbActor.getTransactionCacheData(tx.baseHash)
    val addressCache = dbActor.getAddressCacheData(tx.src.hash)
    TransactionValidationStatus(tx, txCache, addressCache)
  }
}
