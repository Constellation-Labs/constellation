package org.constellation.consensus

import java.util.concurrent.TimeUnit
import akka.util.Timeout

import org.constellation.datastore.Datastore
import org.constellation.primitives.Schema.{AddressCacheData, Transaction, TransactionCacheData}

// TODO: Needs revisiting, data model needs to be updated to incorporate validating diff since snapshot
// Validation by a state only works as a rough pre-filter on memPool, but still is useful

// doc
object Validation {

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  // TODO : Add an LRU cache for looking up TransactionCacheData instead of pure LDB calls.

  // doc
  case class CheckpointValidationStatus(
                                       )

  /** Transaction validation status class
    *
    * @todo: Documentation.
    * @todo: Need separate validator here for CB validation vs. mempool addition
    * @todo: I.e. don't check mempool balance when validating a CB because it takes precedence over
    * @todo: a new TX which is being added to mempool and conflicts with current mempool values.
    */

  case class TransactionValidationStatus(
                                          transaction: Transaction,
                                          transactionCacheData: Option[TransactionCacheData],
                                          addressCacheData: Option[AddressCacheData]
                                        ) {

    // doc
    def isDuplicateHash: Boolean = transactionCacheData.exists {
      _.inDAG
    }

    // doc
    def sufficientBalance: Boolean = addressCacheData.exists { c =>
      c.balance >= transaction.amount
    }

    // doc
    def sufficientMemPoolBalance: Boolean = addressCacheData.exists { c =>
      c.memPoolBalance >= transaction.amount
    }

    // doc
    def validByCurrentState: Boolean = !isDuplicateHash && sufficientBalance

    // doc
    def validByCurrentStateMemPool: Boolean = !isDuplicateHash && sufficientBalance && sufficientMemPoolBalance

    // doc
    def validByAncestor(ancestorHash: String): Boolean = {
      transactionCacheData.exists { t => !t.inDAGByAncestor.contains(ancestorHash) } &&
        addressCacheData.exists(_.ancestorBalances.get(ancestorHash).exists(_ >= transaction.amount))
    }

  } // end TransactionValidationStatus class

  /** Check if a transaction already exists on chain or if there's a sufficient balance to process it.
    *
    * @param dbActor : Reference to LevelDB DAO
    * @param tx      : Resolved transaction
    * @return Future of whether or not the transaction should be considered valid.
    * @note A transaction should only be considered in the DAG once it has been committed to a checkpoint block.
    * @note Before that, it exists only in the memPool and is not stored in the database.
    * @todo: Use a bloom filter.
    */
  def validateTransaction(dbActor: Datastore, tx: Transaction): TransactionValidationStatus = {
    val txCache = dbActor.getTransactionCacheData(tx.baseHash)
    val addressCache = dbActor.getAddressCacheData(tx.src.hash)
    TransactionValidationStatus(tx, txCache, addressCache)
  }

} // end Validation object
