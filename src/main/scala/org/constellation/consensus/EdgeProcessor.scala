package org.constellation.consensus

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import org.constellation.Data
import org.constellation.LevelDB.{DBGet, DBPut, DBUpdate}
import org.constellation.primitives.Schema._
import akka.pattern.ask
import Validation.TransactionValidationStatus
import akka.util.Timeout
import EdgeProcessor.{HandleTransaction, signFlow, validByAncestors, _}
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.Consensus.{CheckpointVote, InitializeConsensusRound, RoundHash}
import org.constellation.primitives._
import constellation._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random
import akka.pattern.ask
import akka.util.Timeout

object EdgeProcessor {

  case class HandleTransaction(tx: Transaction)

  val logger = Logger(s"EdgeProcessor")
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  def signFlow(dao: Data, cb: CheckpointBlock): Unit = {
    // Mock
    // Check to see if we should add our signature to the CB
    val cbPrime = updateCheckpointWithSelfSignatureEmit(cb, dao)
    // Add to memPool or update an existing hash with new signatures and check for signature threshold
    updateCheckpointMergeMemPool(cbPrime, dao)

    attemptFormCheckpointUpdateState(dao)
  }

  def validByAncestors(transactions: Seq[TransactionValidationStatus], cb: CheckpointBlock): Boolean =
    transactions.nonEmpty && transactions.forall { s: TransactionValidationStatus =>
      cb.checkpoint.edge.parentHashes.forall { ancestorHash =>
        s.validByAncestor(ancestorHash)
    }
  }

  def validateCheckpointBlock(dao: Data, cb: CheckpointBlock)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    val allTransactions: Future[Seq[TransactionValidationStatus]] = Future.sequence(cb.transactions.map { tx =>
      Validation.validateTransaction(dao.dbActor, tx)
      })

    val isValidated = allTransactions.map { transactions =>
      val validatedByTransactions = transactions.forall(_.validByCurrentState)//TODO: Validate the checkpoint to see if any there are any duplicate transactions
      //Todo we also need a batch transaction validator, i.e. take cpb, take all tx, group by source address, sum ammts per address (reducebykey) then query balance
      if (validatedByTransactions) {
        signFlow(dao, cb)
        validatedByTransactions
      }
      else {
        val validatedByAncestors = validByAncestors(transactions, cb)
        if (validatedByAncestors) {}//todo resolveAncestryConflict(cb)
        validatedByAncestors
      }
    }
    isValidated
  }

  def handleCheckpoint(cb: CheckpointBlock,
                       dao: Data,
                       internalMessage: Boolean = false)(implicit executionContext: ExecutionContext): Unit = {
    if (!internalMessage) {
      dao.metricsManager ! IncrementMetric("checkpointMessages")
    } else {
      dao.metricsManager ! IncrementMetric("internalCheckpointMessages")
    }
    val potentialChildren: Option[Seq[CheckpointBlock]] = dao.resolveNotifierCallbacks.get(cb.soeHash)
    val cache: Future[Option[CheckpointCacheData]] = dao.hashToCheckpointCacheData(cb.baseHash)
    val parentCache = Future.sequence(cb.checkpoint.edge.parentHashes
      .map { h => dao.hashToSignedObservationEdgeCache(h).map{h -> _} }
    )
    cache.foreach {
      checkpointCacheData: Option[CheckpointCacheData] =>
      if (checkpointCacheData.isDefined) {
        checkpointCacheData.foreach(handleDupTx(_, cb, dao))
      }
      else {
        Resolve.resolveCheckpoint(dao, cb).map { r =>
          if (r) {
            dao.metricsManager ! IncrementMetric("resolvedCheckpointMessages")
            validateCheckpointBlock(dao, cb)
            //          .onComplete {
            //            potentialChildren.foreach {//todo process children
            //              _.foreach { c =>
            //                Future(handleCheckpoint(c, dao, true))
            //              }
            //            }
            //          }
            // Also need to register it with ldb ? To prevent duplicate processing ? Or after
            // If there are duplicate transactions, do a score calculation relative to the other one to determine which to preserve.
            // Potentially rolling back the other one.
          }
          else {
            dao.metricsManager ! IncrementMetric("unresolvedCheckpointMessages")
          }
        }
      }
    }//
  }
    def handleDupTx(ca: CheckpointCacheData, cb: CheckpointBlock, dao: Data) = {
      dao.metricsManager ! IncrementMetric("dupCheckpointReceived")
      if (ca.resolved) {
        if (ca.inDAG) {
          if (ca.checkpointBlock != cb) {
            // Data mismatch on base hash lookup, i.e. signature conflict
            // TODO: Conflict resolution
            //resolveConflict(cb, ca)
          } else {
            // Duplicate checkpoint message, no action required.
          }
        } else {
          // warn or store information about potential conflict
          // if block is not yet in DAG then it doesn't matter, can just store updated value or whatever
        }
      } else {

        // Data is stored but not resolved, potentially trigger resolution check to see if something failed?
        // Otherwise do nothing as the resolution is already in progress.

      }

    }

  // TODO : Add checks on max number in mempool and max num signatures.
  // TEMPORARY mock-up for pre-consensus integration mimics transactions
  def updateCheckpointWithSelfSignatureEmit(cb: CheckpointBlock, dao: Data): CheckpointBlock = {
    val cbPrime = if (!cb.signatures.exists(_.publicKey == dao.keyPair.getPublic)) {
      // We haven't yet signed this CB
      val cb2 = cb.plus(dao.keyPair)
      // Send peers new signature
      dao.peerManager ! APIBroadcast(_.put(s"checkpoint/${cb.baseHash}", cb2))
      cb2
    } else {
      // We have already signed this CB,
      cb
    }
    cbPrime
  }

  // TEMPORARY mock-up for pre-consensus integration mimics transactions
  def updateCheckpointMergeMemPool(cb: CheckpointBlock, dao: Data) : Unit = {
    val cbPostUpdate = if (dao.checkpointMemPool.contains(cb.baseHash)) {
      // Merge signatures together
      val updated = dao.checkpointMemPool(cb.baseHash).plus(cb)
      // Update memPool with new signatures.
      dao.checkpointMemPool(cb.baseHash) = updated
      updated
    }
    else {
      dao.checkpointMemPool(cb.baseHash) = cb
      cb
    }

    // Check to see if we have enough signatures to include in CB
    if (cbPostUpdate.signatures.size >= dao.minCBSignatureThreshold) {
      // Set threshold as met
      dao.checkpointMemPoolThresholdMet(cb.baseHash) = 0
      // Accept transactions
      cb.transactions.foreach { t =>
        t.store(
          dao.dbActor,
          TransactionCacheData(
            t,
            valid = true,
            inMemPool = false,
            inDAG = true,
            Map(cb.baseHash -> true),
            resolved = true,
            cbBaseHash = Some(cb.baseHash)
          ))
        t.ledgerApply(dao.dbActor)
      }
      cb.store(
        dao.dbActor,
        CheckpointCacheData(
          cb,
          inDAG = true,
          resolved = true
        ),
        resolved = true
      )
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
      tx.ledgerApplyMemPool(dao.dbActor)
      dao.transactionMemPool(tx.baseHash) = tx
      tx
    }

    // Check to see if we have enough signatures to include in CB
    if (txPostUpdate.signatures.size >= dao.minTXSignatureThreshold) {
      // Set threshold as met
      dao.transactionMemPoolThresholdMet += tx.baseHash
    }
  }

  def attemptFormCheckpointUpdateState(dao: Data): Option[CheckpointBlock] = {

    if (dao.canCreateCheckpoint) {
      // Form new checkpoint block.

      // TODO : Validate this batch doesn't have a double spend, if it does,
      // just drop all conflicting.

      val tips = Random.shuffle(dao.checkpointMemPoolThresholdMet.toSeq).take(2)

      val tipSOE = tips.map {_._1}.map {dao.checkpointMemPool}.map {
        _.checkpoint.edge.signedObservationEdge
      }

      val checkpointEdgeProposal = createCheckpointEdgeProposal(
        dao.transactionMemPoolThresholdMet,
        dao.minCheckpointFormationThreshold,
        tipSOE
      )(dao.keyPair)

      val takenTX = checkpointEdgeProposal.transactionsUsed.map{dao.transactionMemPool}

      // TODO: move to mem pool servictransactionMemPool
      // Remove used transactions from memPool
      checkpointEdgeProposal.transactionsUsed.foreach{dao.transactionMemPool.remove}

      // Remove threshold transaction hashes
      dao.transactionMemPoolThresholdMet = checkpointEdgeProposal.updatedTransactionMemPoolThresholdMet

      // TODO: move to tips service
      // Update tips
      //dao.validationTips = checkpointEdgeProposal.filteredValidationTips
      tips.foreach { case (tipHash, numUses) =>

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

      val cbBaseHash = checkpointBlock.baseHash

      dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

      dao.checkpointMemPool(cbBaseHash) = checkpointBlock

      // Temporary bypass to consensus for mock
      // Send all data (even if this is redundant.)
      dao.peerManager ! APIBroadcast(_.put(s"checkpoint/$cbBaseHash", checkpointBlock))

      Some(checkpointBlock)
    } else None
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

    val validationResult = Validation.validateTransaction(dao.dbActor, tx)

    val finished = Await.result(validationResult, 90.seconds)

    finished match {
      // TODO : Increment metrics here for each case
      case t : TransactionValidationStatus if t.validByCurrentState =>

        // Check to see if we should add our signature to the transaction
        val txPrime = updateWithSelfSignatureEmit(tx, dao)

        // Add to memPool or update an existing hash with new signatures and check for signature threshold
        updateMergeMemPool(txPrime, dao)

        //        triggerCheckpointBlocking(dao, txPrime)
        var txStatusUpdatedInDB : Boolean = false
        val checkpointBlock = attemptFormCheckpointUpdateState(dao)

        if (checkpointBlock.exists{_.transactions.contains(tx)}) {
          txStatusUpdatedInDB = true
        }

        if (!txStatusUpdatedInDB && t.transactionCacheData.isEmpty) {
          // TODO : Store something here for status queries. Make sure it doesn't cause a conflict
          //tx.edge.storeData(dao.dbActor) // This call can always overwrite no big deal
          // dao.dbActor ! DBUpdate//(tx.baseHash)
        }

        dao.metricsManager ! UpdateMetric("transactionMemPool", dao.transactionMemPool.size.toString)
        dao.metricsManager ! UpdateMetric("transactionMemPoolThreshold", dao.transactionMemPoolThresholdMet.size.toString)

      case t : TransactionValidationStatus =>

        // TODO : Add info somewhere so node can find out transaction was invalid on a callback
        reportInvalidTransaction(dao: Data, t: TransactionValidationStatus)
    }

  }

  def reportInvalidTransaction(dao: Data, t: TransactionValidationStatus): Unit = {
    dao.metricsManager ! IncrementMetric("invalidTransactions")
    if (t.isDuplicateHash) {
      dao.metricsManager ! IncrementMetric("hashDuplicateTransactions")
    }
    if (!t.sufficientBalance) {
      dao.metricsManager ! IncrementMetric("insufficientBalanceTransactions")
    }
  }

  def triggerCheckpointBlocking(dao: Data,
                                tx: Transaction)(implicit timeout: Timeout, executionContext: ExecutionContext): Unit = {
    if (dao.canCreateCheckpoint) {

      println(s"starting checkpoint blocking")

      val checkpointBlock = attemptFormCheckpointUpdateState(dao).get

      dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

      val cbBaseHash = checkpointBlock.baseHash
      dao.checkpointMemPool(cbBaseHash) = checkpointBlock

      // TODO: should be subset
      val facilitators = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().keySet

      // TODO: what is the round hash based on?
      // what are thresholds and checkpoint selection

      val obe = checkpointBlock.checkpoint.edge.observationEdge

      val roundHash = RoundHash(obe.left.hash + obe.right.hash)

      // Start check pointing consensus round
      dao.consensus ! InitializeConsensusRound(facilitators, roundHash, (result) => {
        println(s"consensus round complete result roundHash = $roundHash, result = $result")
        EdgeProcessor.handleCheckpoint(result.checkpointBlock, dao)
      }, CheckpointVote(checkpointBlock))

    }
  }

  case class CreateCheckpointEdgeResponse(
                                           checkpointEdge: CheckpointEdge,
                                           transactionsUsed: Set[String],
                                           // filteredValidationTips: Seq[SignedObservationEdge],
                                           updatedTransactionMemPoolThresholdMet: Set[String]
                                         )

  def createCheckpointEdgeProposal(
                                    transactionMemPoolThresholdMet: Set[String],
                                    minCheckpointFormationThreshold: Int,
                                    tips: Seq[SignedObservationEdge],
                                  )(implicit keyPair: KeyPair): CreateCheckpointEdgeResponse = {

    val transactionsUsed: Set[String] = transactionMemPoolThresholdMet.take(minCheckpointFormationThreshold)
    val updatedTransactionMemPoolThresholdMet = transactionMemPoolThresholdMet -- transactionsUsed

    val checkpointEdgeData = CheckpointEdgeData(transactionsUsed.toSeq.sorted)

    //val tips = validationTips.take(2)
    //val filteredValidationTips = validationTips.filterNot(tips.contains)

    val observationEdge = ObservationEdge(
      TypedEdgeHash(tips.head.hash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(Edge(observationEdge, soe, ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData))))

    CreateCheckpointEdgeResponse(checkpointEdge, transactionsUsed,
      //filteredValidationTips,
      updatedTransactionMemPoolThresholdMet)
  }

}

class EdgeProcessor(keyPair: KeyPair, dao: Data)
                   (implicit timeout: Timeout, executionContext: ExecutionContext) extends Actor with ActorLogging {

  implicit val sys: ActorSystem = context.system
  implicit val kp: KeyPair = keyPair

  def receive: Receive = {

    case HandleTransaction(transaction) =>
      log.debug(s"handle transaction = $transaction")

      handleTransaction(transaction, dao)
  }

}
