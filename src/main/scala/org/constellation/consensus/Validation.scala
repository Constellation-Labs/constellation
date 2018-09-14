package org.constellation.consensus

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import org.constellation.LevelDB.DBGet
import org.constellation.primitives.Schema
import org.constellation.primitives.Schema.{AddressCacheData, Transaction, TransactionCacheData}

import scala.concurrent.{ExecutionContext, Future}


object Validation {


  def validateCheckpoint(dbActor: ActorRef, cb: Schema.CheckpointBlock)(implicit ec: ExecutionContext): Future[CheckpointValidationStatus] = {
    Future{CheckpointValidationStatus()}
  }


  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  // TODO : Add an LRU cache for looking up TransactionCacheData instead of pure LDB calls.

  case class CheckpointValidationStatus(
                                        )

  case class TransactionValidationStatus(
                                        transaction: Transaction,
                                        transactionCacheData: Option[TransactionCacheData],
                                        addressCacheData: Option[AddressCacheData]
                                        ) {
    def isDuplicateHash: Boolean = transactionCacheData.exists{_.inDAG}
    def sufficientBalance: Boolean = addressCacheData.exists{c =>
      c.balance >= transaction.amount && c.memPoolBalance >= transaction.amount
    }
    def validByCurrentState: Boolean = !isDuplicateHash && sufficientBalance
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
  def validateTransaction(dbActor: ActorRef, tx: Transaction)(implicit ec: ExecutionContext): Future[TransactionValidationStatus] = {

    // A transaction should only be considered in the DAG once it has been committed to a checkpoint block.
    // Before that, it exists only in the memPool and is not stored in the database.
    val txCache = (dbActor ? DBGet(tx.baseHash)).mapTo[Option[TransactionCacheData]] //.map{_.exists{_.inDAG}}
    val addressCache = (dbActor ? DBGet(tx.src.hash)).mapTo[Option[AddressCacheData]] //.map(_.exists(_.balance >= tx.amount))
    txCache.flatMap{ txc =>
      addressCache.map{ ac =>
        TransactionValidationStatus(tx, txc, ac)
      }
    }
  }
}
