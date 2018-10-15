package org.constellation.consensus

import org.constellation.DAO
import org.constellation.primitives.Schema.Transaction
import org.constellation.primitives.{APIBroadcast, IncrementMetric}

object TXDependencyWitness {

  // TODO: Re-enable this section later, turning off for now for simplicity
  // Required later for dependency blocks / app support
  /**
    * Potentially add our signature to a transaction and if we haven't yet signed it emit to peers
    * @param tx : Transaction
    * @param dao : Data access object
    * @return Maybe updated transaction
    */
  def updateWithSelfSignatureEmit(tx: Transaction, dao: DAO): Transaction = {
    val txPrime = if (!tx.signatures.exists(_.publicKey == dao.keyPair.getPublic)) {
      // We haven't yet signed this TX
      val tx2 = tx.plus(dao.keyPair)
      dao.metricsManager ! IncrementMetric("signaturesPerformed")
      // Send peers new signature
      dao.peerManager ! APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx2))
      dao.metricsManager ! IncrementMetric("transactionBroadcasts")
      tx2
    } else {
      // We have already signed this transaction,
      tx
    }
    txPrime
  }

  // TODO: Re-enable this section later, turning off for now for simplicity
  // Required later for dependency blocks / app support
  /**
    * Check the memPool to see if signatures are already stored under same OE hash,
    * if so, add current signatures to existing ones. Otherwise, store this in memPool
    * @param tx : Transaction after self signature added
    * @param dao : Data access object
    * @return : Potentially updated transaction.
    */
  def updateMergeMemPool(tx: Transaction, dao: DAO) : Unit = {
    val txPostUpdate = if (dao.transactionMemPoolMultiWitness.contains(tx.baseHash)) {
      // Merge signatures together
      val updated = dao.transactionMemPoolMultiWitness(tx.baseHash).plus(tx)
      // Update memPool with new signatures.
      dao.transactionMemPoolMultiWitness(tx.baseHash) = updated
      updated
    }
    else {
      tx.ledgerApplyMemPool(dao.dbActor)
      dao.transactionMemPoolMultiWitness(tx.baseHash) = tx
      tx
    }

    // Check to see if we have enough signatures to include in CB
    if (txPostUpdate.signatures.size >= dao.minTXSignatureThreshold) {
      // Set threshold as met
      dao.transactionMemPoolThresholdMet += tx.baseHash
    }
  }

}
