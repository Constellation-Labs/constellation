package org.constellation.consensus

import com.typesafe.scalalogging.Logger
import org.constellation.Data
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, EdgeService, IncrementMetric, UpdateMetric}

import scala.concurrent.ExecutionContext


object EdgeProcessor {


  val logger = Logger(s"EdgeProcessor")

  def handleCheckpoint(cb: CheckpointBlock, dao: Data, internalMessage: Boolean = false)(implicit executionContext: ExecutionContext): Unit = {

    if (!internalMessage) {
      dao.metricsManager ! IncrementMetric("checkpointMessagesReceived")
    } else {
      dao.metricsManager ! IncrementMetric("internalCheckpointMessagesReceived")
    }

    Resolve.resolveCheckpoint(dao, cb)

  }

  // TODO : Add checks on max number in mempool and max num signatures.

  /**
    * Potentially add our signature to a transaction and if we haven't yet signed it emit to peers
    * @param tx : Transaction
    * @param dao : Data access object
    * @return Maybe updated transaction
    */
  def updateWithSelfSignatureEmit(tx: Transaction, dao: Data): Transaction = {
    if (!tx.signatures.exists(_.publicKey == dao.keyPair.getPublic)) {
      // We haven't yet signed this TX
      val tx2 = tx.plus(dao.keyPair)
      // Send peers new signature
      dao.peerManager ! APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx2))
      tx2
    } else {
      // We have already signed this transaction,
      tx
    }
  }

  /**
    * Check the memPool to see if signatures are already stored under same OE hash,
    * if so, add current signatures to existing ones. Otherwise, store this in memPool
    * @param tx : Transaction after self signature added
    * @param dao : Data access object
    * @return : Potentially updated transaction.
    */
  def updateMergeMemPool(tx: Transaction, dao: Data) : Unit = {
    val txPostUpdate = if (dao.transactionMemPool.contains(tx.hash)) {
      // Merge signatures together
      val updated = dao.transactionMemPool(tx.hash).plus(tx)
      // Update memPool with new signatures.
      dao.transactionMemPool(tx.hash) = updated
      updated
    }
    else {
      dao.transactionMemPool(tx.hash) = tx
      tx
    }

    // Check to see if we have enough signatures to include in CB
    if (txPostUpdate.signatures.size >= dao.minTXSignatureThreshold) {
      // Set threshold as met
      dao.transactionMemPoolThresholdMet += tx.hash
    }
  }

  def formCheckpointUpdateState(dao: Data, tx: Transaction): CheckpointBlock = {

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

    val checkpointEdgeProposal = EdgeService.createCheckpointEdgeProposal(
      dao.transactionMemPoolThresholdMet,
      dao.minCheckpointFormationThreshold,
      dao.validationTips
    )(dao.keyPair)

    val takenTX = checkpointEdgeProposal.transactionsUsed.map{dao.transactionMemPool}

    // TODO: move to mem pool service
    // Remove used transactions from memPool
    checkpointEdgeProposal.transactionsUsed.foreach{dao.transactionMemPool.remove}
    // Remove threshold transaction hashes
    dao.transactionMemPoolThresholdMet = checkpointEdgeProposal.updatedTransactionMemPoolThresholdMet

    // TODO: move to tips service
    // Update tips
    dao.validationTips = checkpointEdgeProposal.filteredValidationTips

    val checkpointBlock = CheckpointBlock(takenTX.toSeq, checkpointEdgeProposal.checkpointEdge)

    val cbBaseHash = checkpointEdgeProposal.checkpointEdge.edge.baseHash

    takenTX.foreach{ t =>
      t.store(dao.dbActor, cbEdgeHash = Some(cbBaseHash))
    }

    checkpointBlock
  }


  /**
    * Main transaction processing cell
    * This is triggered upon external receipt of a transaction. Assume that the transaction being processed
    * came from a peer, not an internal operation.
    * @param tx : Transaction with all data
    * @param dao : Data access object for referencing memPool and other actors
    * @param executionContext : Threadpool to execute transaction processing against. Should be separate
    *                         from other pools for processing different operations.
    */
  def handleTransaction(t: TransactionValidationStatus,
                        tx: Transaction, dao: Data
                       )(implicit executionContext: ExecutionContext): Unit = t match {
    case t: TransactionValidationStatus if t.valid =>
      // Check to see if we should add our signature to the transaction
      val txPrime = updateWithSelfSignatureEmit(tx, dao)
      // Add to memPool or update an existing hash with new signatures and check for signature threshold
      updateMergeMemPool(txPrime, dao)
      if (dao.canCreateCheckpoint) handleCheckpoint(dao, tx, t)

    case t: TransactionValidationStatus => reportInvalidTransaction(dao: Data, t: TransactionValidationStatus)
      // TODO : Add info somewhere so node can find out transaction was invalid on a callback
  }

  def reportInvalidTransaction(dao: Data, t: TransactionValidationStatus) = {
    dao.metricsManager ! IncrementMetric("invalidTransactions")
    if (t.isDuplicateHash) {
      dao.metricsManager ! IncrementMetric("hashDuplicateTransactions")
    }
    if (!t.sufficientBalance) {
      dao.metricsManager ! IncrementMetric("insufficientBalanceTransactions")
    }
  }

  def handleCheckpoint(dao: Data, tx: Transaction, t: TransactionValidationStatus): Unit = {
    val checkpointBlock = formCheckpointUpdateState(dao, tx)
    dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")
    val cbBaseHash = checkpointBlock.hash
    dao.checkpointMemPool(cbBaseHash) = checkpointBlock.checkpoint
    // Temporary bypass to consensus for mock
    // Send all data (even if this is redundant.)
    dao.peerManager ! APIBroadcast(_.put(s"checkpoint/$cbBaseHash", checkpointBlock))
    if (checkpointBlock.transactions.contains(t.transaction)) {
      // TODO : Add info to DB about transaction status for async reporting info ? Potentially ?
      // Or deal with elsewhere? Either way need to update something so node can figure out what's happening with TX
    }
    dao.metricsManager ! UpdateMetric("transactionMemPool", dao.transactionMemPool.size.toString)
    dao.metricsManager ! UpdateMetric("transactionMemPoolThreshold", dao.transactionMemPoolThresholdMet.size.toString)
  }
}
