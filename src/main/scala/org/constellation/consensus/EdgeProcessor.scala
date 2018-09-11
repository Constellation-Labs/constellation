package org.constellation.consensus

import java.security.KeyPair

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import org.constellation.Data
import org.constellation.LevelDB.DBPut
import org.constellation.primitives.Schema._
import akka.pattern.ask
import Validation.TransactionValidationStatus
import akka.util.Timeout
import EdgeProcessor._
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.Consensus.{CheckpointVote, InitializeConsensusRound, RoundHash}
import org.constellation.consensus.EdgeProcessor.HandleTransaction
import org.constellation.primitives._
import org.constellation.util.SignHelp
import constellation._

import scala.concurrent.ExecutionContext

object EdgeProcessor {

  case class HandleTransaction(tx: Transaction)

  val logger = Logger(s"EdgeProcessor")

  def handleCheckpoint(cb: CheckpointBlock,
                       dao: Data,
                       internalMessage: Boolean = false)(implicit executionContext: ExecutionContext): Unit = {

    if (!internalMessage) {
      dao.metricsManager ! IncrementMetric("checkpointMessagesReceived")
    } else {
      dao.metricsManager ! IncrementMetric("internalCheckpointMessagesReceived")
    }

    // TODO temp store

    dao.checkpointTips.+:(cb.checkpoint)

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

    val checkpointEdgeProposal = createCheckpointEdgeProposal(
      dao.transactionMemPoolThresholdMet,
      dao.minCheckpointFormationThreshold,
      dao.validationTips
    )(dao.keyPair)

    val takenTX = checkpointEdgeProposal.transactionsUsed.map{dao.transactionMemPool}

    // TODO: move to mem pool servictransactionMemPoole
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
  def handleTransaction(tx: Transaction,
                        dao: Data)(implicit executionContext: ExecutionContext, timeout: Timeout) = {

    // TODO: Store TX in DB and during signing updates delete the old SOE ? Or clean it up later?
    // SOE will appear multiple times as signatures are added together.

    dao.metricsManager ! IncrementMetric("transactionMessagesReceived")

    // Validate transaction TODO : This can be more efficient, calls get repeated several times
    // in event where a new signature is being made by another peer it's most likely still valid, should
    // cache the results of this somewhere.

    Validation.validateTransaction(dao.dbActor, tx).foreach {
      // TODO : Increment metrics here for each case
      case t: TransactionValidationStatus if t.valid =>

        println(s"validated, doing other things $tx")

        // Check to see if we should add our signature to the transaction
        val txPrime = updateWithSelfSignatureEmit(tx, dao)

        // Add to memPool or update an existing hash with new signatures and check for signature threshold
        updateMergeMemPool(txPrime, dao)

        triggerCheckpointBlocking(dao, txPrime)

        dao.metricsManager ! UpdateMetric("transactionMemPool", dao.transactionMemPool.size.toString)
        dao.metricsManager ! UpdateMetric("transactionMemPoolThreshold", dao.transactionMemPoolThresholdMet.size.toString)

      case t: TransactionValidationStatus =>
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

  def reportInvalidTransaction(dao: Data, t: TransactionValidationStatus) = {
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

      val checkpointBlock = formCheckpointUpdateState(dao, tx)

      dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

      val cbBaseHash = checkpointBlock.hash

      dao.checkpointMemPool(cbBaseHash) = checkpointBlock.checkpoint

      // TODO: should be subset
      val facilitators = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().keySet

      println(s"finally got facilitators")

      // TODO: what is the round hash based on?
      // what are thresholds and checkpoint selection

      val roundHash = RoundHash("temp")

      // Start check pointing consensus round
      dao.consensus ! InitializeConsensusRound(facilitators, roundHash, (result) => {
        println(s"consensus round complete result roundHash = $roundHash, result = $result")
        EdgeProcessor.handleCheckpoint(result.checkpointBlock, dao)
      }, CheckpointVote(checkpointBlock))

    }
  }

  case class CreateCheckpointEdgeResponse(checkpointEdge: CheckpointEdge,
                                          transactionsUsed: Set[String],
                                          filteredValidationTips: Seq[SignedObservationEdge],
                                          updatedTransactionMemPoolThresholdMet: Set[String])

  def createCheckpointEdgeProposal(transactionMemPoolThresholdMet: Set[String],
                                   minCheckpointFormationThreshold: Int,
                                   validationTips: Seq[SignedObservationEdge])(implicit keyPair: KeyPair): CreateCheckpointEdgeResponse = {

    val transactionsUsed = transactionMemPoolThresholdMet.take(minCheckpointFormationThreshold)
    val updatedTransactionMemPoolThresholdMet = transactionMemPoolThresholdMet -- transactionsUsed

    val checkpointEdgeData = CheckpointEdgeData(transactionsUsed.toSeq.sorted)

    val tips = validationTips.take(2)
    val filteredValidationTips = validationTips.filterNot(tips.contains)

    val observationEdge = ObservationEdge(
      TypedEdgeHash(tips.head.hash, EdgeHashType.ValidationHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.ValidationHash),
      data = Some(TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
    )

    val signedObservationEdge = SignHelp.signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(Edge(observationEdge, signedObservationEdge,
      ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData))))

    CreateCheckpointEdgeResponse(checkpointEdge, transactionsUsed,
      filteredValidationTips, updatedTransactionMemPoolThresholdMet)
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
