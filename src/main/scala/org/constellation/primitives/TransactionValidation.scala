package org.constellation.primitives

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.util.Timeout
import org.constellation.datastore.Datastore
import org.constellation.primitives.Schema.Transaction
import scalaj.http.HttpResponse

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
  def returnIfValid(dbActor: Datastore, keyPair: KeyPair, peerManager: ActorRef, metricsManager: ActorRef)(tx: Transaction): Future[Transaction] = {

    // A transaction should only be considered in the DAG once it has been committed to a checkpoint block.
    // Before that, it exists only in the memPool and is not stored in the database.
    val isDuplicate = dbActor.getTransactionCacheData(tx.hash).exists(_.inDAG)

    val sufficientBalance =
      dbActor.getAddressCacheData(tx.src.hash).exists(_.balance >= tx.amount)

    Future {
      if (!isDuplicate && sufficientBalance) {

        // Check to see if we should add our signature to the transaction
        if (!tx.signatures.exists(_.publicKey == keyPair.getPublic)) {
          // We haven't yet signed this TX
          val tx2 = tx.plus(keyPair)
          // Send peers new signature
          val broadcast
            : APIBroadcast[Future[HttpResponse[String]]] = APIBroadcast(
            _.put(
              s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}",
              tx2
            )
          )
          peerManager ! broadcast
          tx2
        } else {
          // We have already signed this transaction,
          tx
        }
      } else {
        throw new RuntimeException("Illegal Transaction")
      }
    }
  }
}
