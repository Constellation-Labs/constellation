package org.constellation.primitives

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import org.constellation.primitives.Schema.{AddressCacheData, Transaction, TransactionCacheData}
import akka.pattern.ask
import akka.util.Timeout
import org.constellation.LevelDB.DBGet
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future


object TransactionValidation {

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)


  // TODO : Add an LRU cache for looking up TransactionCacheData instead of pure LDB calls.

  /**
    * Check if a transaction already exists on chain or if there's a sufficient balance to process it
    * TODO: Use a bloom filter
    * @param dbActor: Reference to LevelDB DAO
    * @param tx : Resolved transaction
    * @return Future of whether or not the transaction should be considered valid
    */
  def validateTransaction(dbActor: ActorRef, tx: Transaction): Future[Boolean] = {

    // A transaction should only be considered in the DAG once it has been committed to a checkpoint block.
    // Before that, it exists only in the memPool and is not stored in the database.
    val isDuplicate = (dbActor ? DBGet(tx.hash)).mapTo[Option[TransactionCacheData]].map{_.exists{_.inDAG}}
    val sufficientBalance = (dbActor ? DBGet(tx.src.hash)).mapTo[Option[AddressCacheData]].map(_.exists(_.balance >= tx.amount))
    Future.sequence(Seq(isDuplicate, sufficientBalance)).map{ case Seq(dupe, balance) => !dupe && balance}
  }

}
