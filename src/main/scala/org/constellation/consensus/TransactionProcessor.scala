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


//  def handleTransaction(
//                         tx: Transaction, dao: Data, txPrime: Transaction
//                       )(implicit executionContext: ExecutionContext): Unit = {
//    // Validate transaction TODO : This can be more efficient, calls get repeated several times
//    // in event where a new signature is being made by another peer it's most likely still valid, should
//    // cache the results of this somewhere.
//
//        // Add to memPool or update an existing hash with new signatures
//        if (!dao.txMemPoolOE.contains(tx.hash)) dao.txMemPoolOE(tx.hash) = txPrime
//        else {
//
//          // Merge signatures together
//          val updated = dao.txMemPoolOE(tx.hash).plus(txPrime)
//          if (updated.signatures.size < minTXSignatureThreshold) dao.txMemPoolOE(tx.hash) = updated
//          // Check to see if we have enough signatures to include in CB
//          else {// Todo This would be our second cell type, which makes a call to formCheckpointBlock and would unwind the result
//
//            // Set threshold as met
//            dao.txMemPoolOEThresholdMet += tx.hash
//            if (dao.txMemPoolOEThresholdMet.size >= minCheckpointFormationThreshold && dao.validationTips.size >= 2) {
//
//              // Form new checkpoint block.
//              val soe: Option[SignedObservationEdge] = formCheckpointBlock(dao)
//              // TODO: Broadcast CB edge
//            }
//          }
//        }
//  }
//
//  def formCheckpointBlock(dao: Data) = {
//    // Form new checkpoint block.
//
//    // TODO : Validate this batch doesn't have a double spend, if it does,
//    // just drop all conflicting.
//
//    val taken = dao.txMemPoolOEThresholdMet.take(minCheckpointFormationThreshold)
//    dao.txMemPoolOEThresholdMet --= taken
//    taken.foreach{dao.txMemPoolOE.remove}
//
//    val ced = CheckpointEdgeData(taken.toSeq.sorted)
//
//    val tips = dao.validationTips.take(2)
//    dao.validationTips = dao.validationTips.filterNot(tips.contains)
//
//    val oe = tips.headOption.map(ObservationEdge(
//      _,
//      tips(1),
//      data = Some(TypedEdgeHash(ced.hash, EdgeHashType.CheckpointDataHash))
//    ))
//
//    oe.map(SignHelp.signedObservationEdge(_)(dao.keyPair))
//
//  }

}
