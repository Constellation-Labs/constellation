package org.constellation.consensus

import akka.actor.ActorRef
import org.constellation.Data
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, IncrementMetric, TransactionValidation}
import org.constellation.util.SignHelp

import scala.concurrent.ExecutionContext


object TransactionProcessor {


  val minTXSignatureThreshold = 3
  val minCheckpointFormationThreshold = 3
  // TODO : Add checks on max number in mempool and max num signatures.
  val maxUniqueTXSize = 500
  val maxNumSignaturesPerTX = 20

  def handleTransaction(
                         tx: Transaction, dao: Data
                       )(implicit executionContext: ExecutionContext): Unit = {
    // Validate transaction TODO : This can be more efficient, calls get repeated several times
    // in event where a new signature is being made by another peer it's most likely still valid, should
    // cache the results of this somewhere.
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
        if (dao.txMemPoolOE.contains(tx.hash)) {

          // Merge signatures together
          val updated = dao.txMemPoolOE(tx.hash).plus(txPrime)

          // Check to see if we have enough signatures to include in CB
          if (updated.signatures.size >= minTXSignatureThreshold) {

            // Set threshold as met
            dao.txMemPoolOEThresholdMet += tx.hash

            if (dao.txMemPoolOEThresholdMet.size >= minCheckpointFormationThreshold && dao.validationTips.size >= 2) {

              // Form new checkpoint block.

              // TODO : Validate this batch doesn't have a double spend, if it does,
              // just drop all conflicting.

              val taken = dao.txMemPoolOEThresholdMet.take(minCheckpointFormationThreshold)
              dao.txMemPoolOEThresholdMet --= taken
              taken.foreach{dao.txMemPoolOE.remove}

              val ced = CheckpointEdgeData(taken.toSeq.sorted)

              val tips = dao.validationTips.take(2)
              dao.validationTips = dao.validationTips.filterNot(tips.contains)

              val oe = ObservationEdge(
                tips.head,
                tips(1),
                data = Some(TypedEdgeHash(ced.hash, EdgeHashType.CheckpointDataHash))
              )

              val soe = SignHelp.signedObservationEdge(oe)(dao.keyPair)

              // TODO: Broadcast CB edge

            }
          }
          dao.txMemPoolOE(tx.hash) = updated
        } else {
          dao.txMemPoolOE(tx.hash) = txPrime
        }

      // Trigger check if we should emit a CB


      case false =>
        dao.metricsManager ! IncrementMetric("invalidTransactions")

    }

  }

}
