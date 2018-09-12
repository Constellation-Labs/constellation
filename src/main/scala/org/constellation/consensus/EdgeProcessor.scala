package org.constellation.consensus

import akka.actor.ActorRef
import org.constellation.Data
import org.constellation.LevelDB.{DBPut, DBUpdate}
import org.constellation.primitives.Schema._
import Validation.TransactionValidationStatus
import com.typesafe.scalalogging.Logger
import org.constellation.primitives.{APIBroadcast, EdgeService, IncrementMetric, UpdateMetric}
import org.constellation.util.SignHelp

import scala.concurrent.ExecutionContext
import scala.util.Random


object EdgeProcessor {


  val logger = Logger(s"EdgeProcessor")

  def handleCheckpoint(cb: CheckpointBlock, dao: Data, internalMessage: Boolean = false)(implicit executionContext: ExecutionContext): Unit = {

    if (!internalMessage) {
      dao.metricsManager ! IncrementMetric("checkpointMessages")
    } else {
      dao.metricsManager ! IncrementMetric("internalCheckpointMessages")
    }

    // TODO: Validate the checkpoint to see if any there are any duplicate transactions
    // Also need to register it with ldb ? To prevent duplicate processing ? Or after
    // If there are duplicate transactions, do a score calculation relative to the other one to determine which to preserve.
    // Potentially rolling back the other one.

    if (Resolve.resolveCheckpoint(dao, cb)) {
      dao.metricsManager ! IncrementMetric("resolvedCheckpointMessages")

      // Mock

/*      // Check to see if we should add our signature to the CB
      val cbPrime = updateCheckpointWithSelfSignatureEmit(cb, dao)

      // Add to memPool or update an existing hash with new signatures and check for signature threshold
      updateCheckpointMergeMemPool(cbPrime, dao)

      var cbStatusUpdatedInDB : Boolean = false

      if (dao.canCreateValidation) {

        val cbUsed = dao.checkpointMemPoolThresholdMet.take(2)
        val updatedTransactionMemPoolThresholdMet = dao.checkpointMemPoolThresholdMet -- cbUsed*/

  /*      val checkpointEdgeData = CheckpointEdgeData(transactionsUsed.toSeq.sorted)

        val tips = validationTips.take(2)
        val filteredValidationTips = validationTips.filterNot(tips.contains)

        val observationEdge = ObservationEdge(
          TypedEdgeHash(tips.head.hash, EdgeHashType.ValidationHash),
          TypedEdgeHash(tips(1).hash, EdgeHashType.ValidationHash),
          data = Some(TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
        )

        val signedObservationEdge = SignHelp.signedObservationEdge(observationEdge)(keyPair)

        val checkpointEdge = CheckpointEdge(Edge(observationEdge, signedObservationEdge, ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData))))
*/

    //  }


      /*



          val checkpointBlock = formCheckpointUpdateState(dao, tx)
          dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

          val cbBaseHash = checkpointBlock.hash
          dao.checkpointMemPool(cbBaseHash) = checkpointBlock.checkpoint

          // Temporary bypass to consensus for mock
          // Send all data (even if this is redundant.)
          dao.peerManager ! APIBroadcast(_.put(s"checkpoint/$cbBaseHash", checkpointBlock))

          if (checkpointBlock.transactions.contains(tx)) {
            txStatusUpdatedInDB = true
          }
        }

        if (!txStatusUpdatedInDB && t.transactionCacheData.isEmpty) {
          tx.store(dao.dbActor, None, inDAG = false)
        }

        dao.metricsManager ! UpdateMetric("transactionMemPool", dao.transactionMemPool.size.toString)
        dao.metricsManager ! UpdateMetric("transactionMemPoolThreshold", dao.transactionMemPoolThresholdMet.size.toString)

       */

    } else {
      dao.metricsManager ! IncrementMetric("unresolvedCheckpointMessages")

    }

  }

  // TODO : Add checks on max number in mempool and max num signatures.

  // TEMPORARY mock-up for pre-consensus integration mimics transactions
  def updateCheckpointWithSelfSignatureEmit(cb: CheckpointBlock, dao: Data): CheckpointBlock = {
    val cbPrime = if (!cb.signatures.exists(_.publicKey == dao.keyPair.getPublic)) {
      // We haven't yet signed this CB
      val cb2 = cb.plus(dao.keyPair)
      // Send peers new signature
      dao.peerManager ! APIBroadcast(_.put(s"checkpoint/${cb.hash}", cb2))
      cb2
    } else {
      // We have already signed this CB,
      cb
    }
    cbPrime
  }

  // TEMPORARY mock-up for pre-consensus integration mimics transactions
  def updateCheckpointMergeMemPool(cb: CheckpointBlock, dao: Data) : Unit = {
    val cbPostUpdate = if (dao.checkpointMemPool.contains(cb.hash)) {
      // Merge signatures together
      val updated = dao.checkpointMemPool(cb.hash).plus(cb)
      // Update memPool with new signatures.
      dao.checkpointMemPool(cb.hash) = updated
      updated
    }
    else {
      dao.checkpointMemPool(cb.hash) = cb
      cb
    }

    // Check to see if we have enough signatures to include in CB
    if (cbPostUpdate.signatures.size >= dao.minCBSignatureThreshold) {
      // Set threshold as met
      dao.checkpointMemPoolThresholdMet += cb.hash
    }
  }

  /**
    * Potentially add our signature to a transaction and if we haven't yet signed it emit to peers
    * @param tx : Transaction
    * @param dao : Data access object
    * @return Maybe updated transaction
    */
  def updateWithSelfSignatureEmit(tx: Transaction, dao: Data): Transaction = {
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
    txPrime
  }

  /**
    * Check the memPool to see if signatures are already stored under same OE hash,
    * if so, add current signatures to existing ones. Otherwise, store this in memPool
    * @param tx : Transaction after self signature added
    * @param dao : Data access object
    * @return : Potentially updated transaction.
    */
  def updateMergeMemPool(tx: Transaction, dao: Data) : Unit = {
    val txPostUpdate = if (dao.transactionMemPool.contains(tx.baseHash)) {
      // Merge signatures together
      val updated = dao.transactionMemPool(tx.baseHash).plus(tx)
      // Update memPool with new signatures.
      dao.transactionMemPool(tx.baseHash) = updated
      updated
    }
    else {
      dao.transactionMemPool(tx.baseHash) = tx
      tx
    }

    // Check to see if we have enough signatures to include in CB
    if (txPostUpdate.signatures.size >= dao.minTXSignatureThreshold) {
      // Set threshold as met
      dao.transactionMemPoolThresholdMet += tx.baseHash
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

    val tips = Random.shuffle(dao.checkpointMemPoolThresholdMet.toSeq).take(2)

    val tipSOE = tips.map{_._1}.map{dao.checkpointMemPool}.map{_.checkpoint.edge.signedObservationEdge}

    val checkpointEdgeProposal = EdgeService.createCheckpointEdgeProposal(
      dao.transactionMemPoolThresholdMet,
      dao.minCheckpointFormationThreshold,
      tipSOE
    )(dao.keyPair)


    val takenTX = checkpointEdgeProposal.transactionsUsed.map{dao.transactionMemPool}

    // TODO: move to mem pool service
    // Remove used transactions from memPool
    checkpointEdgeProposal.transactionsUsed.foreach{dao.transactionMemPool.remove}
    // Remove threshold transaction hashes
    dao.transactionMemPoolThresholdMet = checkpointEdgeProposal.updatedTransactionMemPoolThresholdMet

    // TODO: move to tips service
    // Update tips
    //dao.validationTips = checkpointEdgeProposal.filteredValidationTips
    tips.foreach{ case (tipHash, numUses) =>

      def doRemove(): Unit = {
        dao.checkpointMemPoolThresholdMet.remove(tipHash)
        dao.checkpointMemPool.remove(tipHash)
      }

      if (dao.reuseTips) {
        if (numUses >= 2) {
          doRemove()
        } else {
          dao.checkpointMemPoolThresholdMet(tipHash) += 1
        }
      } else {
        doRemove()
      }
    }

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
        val txPrime = updateWithSelfSignatureEmit(tx, dao)

        // Add to memPool or update an existing hash with new signatures and check for signature threshold
        updateMergeMemPool(txPrime, dao)

        var txStatusUpdatedInDB : Boolean = false

        if (dao.canCreateCheckpoint) {

          val checkpointBlock = formCheckpointUpdateState(dao, tx)
          dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

          val cbBaseHash = checkpointBlock.hash
          dao.checkpointMemPool(cbBaseHash) = checkpointBlock

          // Temporary bypass to consensus for mock
          // Send all data (even if this is redundant.)
          dao.peerManager ! APIBroadcast(_.put(s"checkpoint/$cbBaseHash", checkpointBlock))

          if (checkpointBlock.transactions.contains(tx)) {
            txStatusUpdatedInDB = true
          }
        }

        if (!txStatusUpdatedInDB && t.transactionCacheData.isEmpty) {
          tx.edge.storeData(dao.dbActor) // This call can always overwrite no big deal
          dao.dbActor ! DBUpdate(tx.baseHash)
        }

        dao.metricsManager ! UpdateMetric("transactionMemPool", dao.transactionMemPool.size.toString)
        dao.metricsManager ! UpdateMetric("transactionMemPoolThreshold", dao.transactionMemPoolThresholdMet.size.toString)

      case t : TransactionValidationStatus =>

        // TODO : Add info somewhere so node can find out transaction was invalid on a callback

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
