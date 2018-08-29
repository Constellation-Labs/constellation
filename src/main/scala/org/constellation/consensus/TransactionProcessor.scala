package org.constellation.consensus

import java.security.KeyPair

import akka.actor.ActorRef
import com.typesafe.scalalogging.Logger
import org.constellation.Data
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, EdgeService, IncrementMetric, TransactionValidation}
import org.constellation.util.SignHelp

import scala.concurrent.ExecutionContext

object TransactionProcessor {
  val logger = Logger(s"TransactionProcessor")

  val minTXSignatureThreshold = 3
  // TODO : Add checks on max number in mempool and max num signatures.
  val maxUniqueTXSize = 500
  val maxNumSignaturesPerTX = 20

  def handleTransaction(tx: Transaction,
                        dao: Data)(implicit executionContext: ExecutionContext, keyPair: KeyPair): Unit = {

    // Validate transaction TODO : This can be more efficient, calls get repeated several times
    // in event where a new signature is being made by another peer it's most likely still valid, should
    // cache the results of this somewhere.

    logger.debug(s"handle transaction = $tx")

    TransactionValidation.validateTransaction(dao.dbActor, tx).foreach{
      // TODO : Increment metrics here for each case
      case true =>

        // Check to see if we should add our signature to the transaction
        val txPrime = if (!tx.signatures.exists(_.publicKey == dao.keyPair.getPublic)) {
          // We haven't yet signed this TX
          val tx2 = tx.plus(dao.keyPair)
          // Send peers new signature
          dao.peerManager ! APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx2))
          tx2
        } else {
          // We have already signed this transaction,
          tx
        }

        // Add to memPool or update an existing hash with new signatures
        if (dao.transactionMemPool.contains(tx.hash)) {

          // Merge signatures together
          val updated = dao.transactionMemPool(tx.hash).plus(txPrime)

            // Check to see if we have enough signatures to include in CB
            if (updated.signatures.size >= minTXSignatureThreshold) {

            // Set threshold as met
            dao.transactionMemPoolThresholdMet += tx.hash

            if (dao.transactionMemPoolThresholdMet.size >= dao.minCheckpointFormationThreshold && dao.validationTips.size >= 2) {

              // Form new checkpoint block.

              // TODO : Validate this batch doesn't have a double spend, if it does,
              // just drop all conflicting.

              // *****************//
              // TODO: wip
              // Checkpoint block proposal

              // initialize checkpointing consensus state
              // send checkpoint block proposal to everyone
              // as new proposals come in route them to the consensus actor
              // once a threshold is met take majority checkpoint edge
              // store it
              // tell people about it

              // below is a single local formation of a checkpoint edge proposal
              // need to wait on majority of other people before accepting it

              val checkpointEdgeProposal =
                EdgeService.createCheckpointEdgeProposal(dao.transactionMemPoolThresholdMet, dao.minCheckpointFormationThreshold, dao.validationTips)

              // TODO: move to mem pool service
              checkpointEdgeProposal.transactionsUsed.foreach{dao.transactionMemPool.remove}

              dao.transactionMemPoolThresholdMet = checkpointEdgeProposal.updatedTransactionMemPoolThresholdMet

              // TODO: move to tips service
              dao.validationTips = checkpointEdgeProposal.filteredValidationTips

              /******************/

            }
          }
          dao.transactionMemPool(tx.hash) = updated
        } else {
          dao.transactionMemPool(tx.hash) = txPrime
        }

      // Trigger check if we should emit a CB

      case false =>
        dao.metricsManager ! IncrementMetric("invalidTransactions")

    }

  }

}
