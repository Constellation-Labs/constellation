package org.constellation.consensus

import akka.actor.ActorRef
import org.constellation.Data
import org.constellation.LevelDB.DBPut
import org.constellation.primitives.Schema._
import Validation.TransactionValidationStatus
import org.constellation.primitives.{APIBroadcast, IncrementMetric, UpdateMetric}
import org.constellation.util.SignHelp

import scala.concurrent.ExecutionContext


object EdgeProcessor {



  def handleCheckpoint(cb: CheckpointBlock, dao: Data, internalMessage: Boolean = false)(implicit executionContext: ExecutionContext): Unit = {

    if (!internalMessage) {
      dao.metricsManager ! IncrementMetric("checkpointMessagesReceived")
    } else {
      dao.metricsManager ! IncrementMetric("internalCheckpointMessagesReceived")
    }

    Resolve.resolveCheckpoint(dao, cb)

  }

  val minTXSignatureThreshold = 3
  val minCheckpointFormationThreshold = 3
  // TODO : Add checks on max number in mempool and max num signatures.
  val maxUniqueTXSize = 500
  val maxNumSignaturesPerTX = 20

  def handleTransaction(
                         tx: Transaction, dao: Data
                       )(implicit executionContext: ExecutionContext): Unit = {

    // TODO: Store TX in DB and during signing updates delete the old SOE ? Or clean it up later?
    // SOE will appear multiple times as signatures are added together.

    dao.metricsManager ! IncrementMetric("transactionMessagesReceived")
    // Validate transaction TODO : This can be more efficient, calls get repeated several times
    // in event where a new signature is being made by another peer it's most likely still valid, should
    // cache the results of this somewhere.
    Validation.validateTransaction(dao.dbActor, tx).foreach{
      // TODO : Increment metrics here for each case
      case t : TransactionValidationStatus if t.valid =>

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
        val txPostUpdate = if (dao.txMemPoolOE.contains(tx.hash)) {

          // Merge signatures together
          val updated = dao.txMemPoolOE(tx.hash).plus(txPrime)
          // Update memPool with new signatures.
          dao.txMemPoolOE(tx.hash) = updated
          updated
        }
        else {
          dao.txMemPoolOE(tx.hash) = txPrime
          txPrime
        }


        // Check to see if we have enough signatures to include in CB
        if (txPostUpdate.signatures.size >= minTXSignatureThreshold) {

          // Set threshold as met
          dao.txMemPoolOEThresholdMet += tx.hash

        }

        // Attempt CB formation

        var takenIncludesThisTX : Boolean = false

        if (dao.txMemPoolOEThresholdMet.size >= minCheckpointFormationThreshold && dao.validationTips.size >= 2) {

          // Form new checkpoint block.

          // TODO : Validate this batch doesn't have a double spend, if it does,
          // just drop all conflicting.

          val taken = dao.txMemPoolOEThresholdMet.take(minCheckpointFormationThreshold)
          dao.txMemPoolOEThresholdMet --= taken
          val takenTX = taken.map{dao.txMemPoolOE}

          if (taken.contains(tx.hash)) {
            takenIncludesThisTX = true
          }

          taken.foreach{dao.txMemPoolOE.remove}

          val ced = CheckpointEdgeData(taken.toSeq.sorted)

          val tips = dao.validationTips.take(2)
          dao.validationTips = dao.validationTips.filterNot(tips.contains)
          dao.metricsManager ! UpdateMetric("activeTips", dao.validationTips.size.toString)

          val oe = ObservationEdge(
            TypedEdgeHash(tips.head.hash, EdgeHashType.ValidationHash),
            TypedEdgeHash(tips(1).hash, EdgeHashType.ValidationHash),
            data = Some(TypedEdgeHash(ced.hash, EdgeHashType.CheckpointDataHash))
          )

          val soe = SignHelp.signedObservationEdge(oe)(dao.keyPair)

          takenTX.foreach{ t =>
            t.store(dao.dbActor, cbEdgeHash = Some(soe.signatureBatch.hash))
          }

          dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

          val resolvedCB = CheckpointEdge(Edge(oe, soe, ResolvedObservationEdge(tips.head, tips(1), Some(ced))))

          val rco = CheckpointBlock(takenTX.toSeq, resolvedCB)

          dao.checkpointMemPool(soe.signatureBatch.hash) = resolvedCB

          dao.peerManager ! APIBroadcast(_.put(s"checkpoint/${soe.signatureBatch.hash}", rco))
        }

        dao.metricsManager ! UpdateMetric("txMemPoolOESize", dao.txMemPoolOE.size.toString)
        dao.metricsManager ! UpdateMetric("txMemPoolOEThresholdMet", dao.txMemPoolOEThresholdMet.size.toString)

        if (!takenIncludesThisTX) {
        }


      case t : TransactionValidationStatus =>
        dao.metricsManager ! IncrementMetric("invalidTransactions")
        if (t.isDuplicateHash) {
          dao.metricsManager ! IncrementMetric("hashDuplicateTransactions")
        }
        if (!t.sufficientBalance) {
          dao.metricsManager ! IncrementMetric("insufficientBalanceTransactions")
        }

    }

  }

}
