package org.constellation.consensus

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorSystem}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.DAO
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.util.{HeartbeatSubscribe, ProductHash}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object SnapshotTrigger {

  case class HandleTransaction(tx: Transaction)
  case class HandleCheckpoint(checkpointBlock: CheckpointBlock)
  case class HandleSignatureRequest(checkpointBlock: CheckpointBlock)

  val logger = Logger(s"EdgeProcessor")
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)


  def acceptCheckpoint(checkpointCacheData: CheckpointCacheData)(implicit dao: DAO): Unit = {

    if (checkpointCacheData.checkpointBlock.isEmpty) {
      dao.metricsManager ! IncrementMetric("acceptCheckpointCalledWithEmptyCB")
    } else {

      val cb = checkpointCacheData.checkpointBlock.get

      val height = cb.calculateHeight()

      val fallbackHeight = if (height.isEmpty) checkpointCacheData.height else height

      if (fallbackHeight.isEmpty) {
        dao.metricsManager ! IncrementMetric("heightEmpty")
      } else {
        dao.metricsManager ! IncrementMetric("heightNonEmpty")
      }

      // Accept transactions
      cb.transactions.foreach { t =>
        dao.metricsManager ! IncrementMetric("transactionAccepted")
        t.store(
          TransactionCacheData(
            t,
            valid = true,
            inMemPool = false,
            inDAG = true,
            Map(cb.baseHash -> true),
            resolved = true,
            cbBaseHash = Some(cb.baseHash)
          ))
        t.ledgerApply()
      }
      dao.metricsManager ! IncrementMetric("checkpointAccepted")
      cb.store(
        CheckpointCacheData(
          Some(cb),
          height = fallbackHeight
        )
      )

    }
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
                         tx: Transaction, skipValidation: Boolean = true
                       )(implicit executionContext: ExecutionContext, dao: DAO): Unit = {

    // TODO: Store TX in DB immediately rather than waiting
    // ^ Requires TX hash partitioning or conflict stuff later + gossiping tx's to wider reach

    dao.metricsManager ! IncrementMetric("transactionMessagesReceived")
    // Validate transaction TODO : This can be more efficient, calls get repeated several times
    // in event where a new signature is being made by another peer it's most likely still valid, should
    // cache the results of this somewhere.

    // TODO: Move memPool update logic to actor and send response back to EdgeProcessor
    //if (!memPool.transactions.contains(tx)) {
    if (dao.threadSafeTXMemPool.put(tx)) {

      //val pool = memPool.copy(transactions = memPool.transactions + tx)
      //dao.metricsManager ! UpdateMetric("transactionMemPool", pool.transactions.size.toString)
      //dao.threadSafeMemPool.put(transaction)

      dao.metricsManager ! UpdateMetric("transactionMemPool", dao.threadSafeTXMemPool.unsafeCount.toString)
      dao.metricsManager ! IncrementMetric("transactionValidMessages")
      formCheckpoint() // TODO: Send to checkpoint formation actor instead
    } else {
      dao.metricsManager ! IncrementMetric("transactionValidMemPoolDuplicateMessages")
      //memPool
    }

    /*    val finished = Validation.validateTransaction(dao.dbActor, tx)



        finished match {
          // TODO : Increment metrics here for each case
          case t : TransactionValidationStatus if t.validByCurrentStateMemPool =>


            // TODO : Store something here for status queries. Make sure it doesn't cause a conflict

          case t : TransactionValidationStatus =>

            // TODO : Add info somewhere so node can find out transaction was invalid on a callback
            reportInvalidTransaction(dao: DAO, t: TransactionValidationStatus)
            memPool
        }

        */

  }

  def reportInvalidTransaction(dao: DAO, t: TransactionValidationStatus): Unit = {
    dao.metricsManager ! IncrementMetric("invalidTransactions")
    if (t.isDuplicateHash) {
      dao.metricsManager ! IncrementMetric("hashDuplicateTransactions")
    }
    if (!t.sufficientBalance) {
      dao.metricsManager ! IncrementMetric("insufficientBalanceTransactions")
    }
  }

  case class CreateCheckpointEdgeResponse(
                                           checkpointEdge: CheckpointEdge,
                                           transactionsUsed: Set[String],
                                           // filteredValidationTips: Seq[SignedObservationEdge],
                                           updatedTransactionMemPoolThresholdMet: Set[String]
                                         )


  def createCheckpointBlock(transactions: Seq[Transaction], tips: Seq[SignedObservationEdge])
                           (implicit keyPair: KeyPair): CheckpointBlock = {

    val checkpointEdgeData = CheckpointEdgeData(transactions.map{_.hash}.sorted)

    val observationEdge = ObservationEdge(
      TypedEdgeHash(tips.head.hash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(observationEdge, soe, ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData)))
    )

    CheckpointBlock(transactions, checkpointEdge)
  }

  def createCheckpointEdgeProposal(
                                    transactionMemPoolThresholdMet: Set[String],
                                    minCheckpointFormationThreshold: Int,
                                    tips: Seq[SignedObservationEdge],
                                  )(implicit keyPair: KeyPair): CreateCheckpointEdgeResponse = {

    val transactionsUsed = transactionMemPoolThresholdMet.take(minCheckpointFormationThreshold)
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
  // TODO: Send facilitator selection data (i.e. criteria) as well for verification

  case class SignatureRequest(checkpointBlock: CheckpointBlock, facilitators: Set[Id])
  case class SignatureResponse(checkpointBlock: CheckpointBlock, facilitators: Set[Id], reRegister: Boolean = false)
  case class FinishedCheckpoint(checkpointCacheData: CheckpointCacheData, facilitators: Set[Id])
  case class FinishedCheckpointResponse(reRegister: Boolean = false)

  // TODO: Move to checkpoint formation actor
  def formCheckpoint()(implicit dao: DAO): Unit = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    val maybeTransactions = dao.threadSafeTXMemPool.pull(dao.minCheckpointFormationThreshold)

    dao.metricsManager ! IncrementMetric("attemptFormCheckpointCalls")

    if (maybeTransactions.isEmpty) {
      dao.metricsManager ! IncrementMetric("attemptFormCheckpointInsufficientTX")
    }

    maybeTransactions.foreach { transactions =>

      val maybeTips = dao.threadSafeTipService.pull()
      if (maybeTips.isEmpty) {
        dao.metricsManager ! IncrementMetric("attemptFormCheckpointInsufficientTipsOrFacilitators")
      }

      maybeTips.foreach { case (tipSOE, facils) =>

        val checkpointBlock = createCheckpointBlock(transactions, tipSOE)(dao.keyPair)
        dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

        val finalFacilitators = facils.keySet

        val response = facils.map { case (facilId, data) =>
          facilId -> tryWithMetric( // TODO: onComplete instead, debugging with other changes first.
            {
              data.client.postSync(s"request/signature", SignatureRequest(checkpointBlock, finalFacilitators + dao.id))
            },
            "formCheckpointPOSTCallSignatureRequest"
          )
        }

        if (response.exists(_._2.isFailure)) {

          dao.metricsManager ! IncrementMetric("formCheckpointSignatureResponseMissing")

        } else {

          val nonFailedResponses = response.mapValues(_.get)

          if (!nonFailedResponses.forall(_._2.isSuccess)) {

            dao.metricsManager ! IncrementMetric("formCheckpointSignatureResponseNot2xx")

          } else {

            val parsed = nonFailedResponses.mapValues(v =>
              tryWithMetric({
                v.body.x[Option[SignatureResponse]]
              }, "formCheckpointSignatureResponseJsonParsing")
            )

            if (parsed.exists(_._2.isFailure)) {

              dao.metricsManager ! IncrementMetric("formCheckpointSignatureResponseExistsParsingFailure")

            } else {

              val withValue = parsed.mapValues(_.get)

              if (withValue.exists(_._2.isEmpty)) {

                dao.metricsManager ! IncrementMetric("formCheckpointSignatureResponseEmpty")

              } else {

                // Successful formation
                dao.metricsManager ! IncrementMetric("formCheckpointSignatureResponseAllResponsesPresent")

                val responseValues = withValue.values.map{_.get}

                responseValues.foreach{ sr =>

                  if (sr.reRegister) {
                    // PeerManager.attemptRegisterPeer() TODO : Finish

                  }

                }

                val blocks = responseValues.map {_.checkpointBlock}

                val finalCB = blocks.reduce { (x: CheckpointBlock, y: CheckpointBlock) => x.plus(y) }.plus(checkpointBlock)

                if (finalCB.signatures.size != finalFacilitators.size + 1) {
                  dao.metricsManager ! IncrementMetric("missingBlockSignatures")
                } else {
                  dao.metricsManager ! IncrementMetric("sufficientBlockSignatures")

                  if (!finalCB.simpleValidation()) {

                    dao.metricsManager ! IncrementMetric("finalCBFailedValidation")

                  } else {
                    dao.metricsManager ! IncrementMetric("finalCBPassedValidation")


                    val cache = CheckpointCacheData(Some(finalCB), height = finalCB.calculateHeight())

                    dao.threadSafeTipService.accept(cache)
                    // TODO: Check failures and/or remove constraint of single actor
                    dao.peerInfo.foreach { case (id, client) =>
                      tryWithMetric(
                        client.client.postSync(s"finished/checkpoint", FinishedCheckpoint(cache, finalFacilitators)),
                        "finishedCheckpointBroadcast"
                      )
                    }
                  }

                }

              }

            }

          }

        }
      }
    }
  }


  // Temporary for testing join/leave logic.
  def handleSignatureRequest(sr: SignatureRequest)(implicit dao: DAO): SignatureResponse = {
    //if (sr.facilitators.contains(dao.id)) {
    // val replyTo = sr.checkpointBlock.witnessIds.head
    val updated = if (sr.checkpointBlock.simpleValidation()) {
      sr.checkpointBlock.plus(dao.keyPair)
    }
    else {
      sr.checkpointBlock
    }
    SignatureResponse(updated, sr.facilitators)
    /*dao.peerManager ! APIBroadcast(
      _.post(s"response/signature", SignatureResponse(updated, sr.facilitators)),
      peerSubset = Set(replyTo)
    )*/
    // } else None
  }

}

case class TipData(checkpointBlock: CheckpointBlock, numUses: Int)

case class SnapshotInfo(
                         snapshot: Snapshot,
                         acceptedCBSinceSnapshot: Seq[String] = Seq(),
                         acceptedCBSinceSnapshotCache: Seq[CheckpointCacheData] = Seq(),
                         lastSnapshotHeight: Int = 0,
                         snapshotHashes: Seq[String] = Seq(),
                         addressCacheData: Map[String, AddressCacheData] = Map(),
                         tips: Map[String, TipData] = Map(),
                         snapshotCache: Seq[CheckpointCacheData] = Seq()
                       )

case object GetMemPool

case class Snapshot(lastSnapshot: String, checkpointBlocks: Seq[String]) extends ProductHash
case class StoredSnapshot(snapshot: Snapshot, checkpointCache: Seq[CheckpointCacheData])

case class DownloadComplete(latestSnapshot: Snapshot)

object Snapshot {

  def triggerSnapshot(round: Long)(implicit dao: DAO): Unit = {
    // TODO: Refactor round into InternalHeartbeat
    if (round % dao.processingConfig.snapshotInterval == 0 && dao.nodeState == NodeState.Ready) {
      futureTryWithTimeoutMetric(
        dao.threadSafeTipService.attemptSnapshot(), "snapshotAttempt"
      )(dao.edgeExecutionContext, dao)
    }
  }



  val snapshotZero = Snapshot("", Seq())
  val snapshotZeroHash: String = Snapshot("", Seq()).hash

  def acceptSnapshot(snapshot: Snapshot)(implicit dao: DAO): Unit = {
    // dao.dbActor.putSnapshot(snapshot.hash, snapshot)
    val cbData = snapshot.checkpointBlocks.map{dao.checkpointService.get}
    if (cbData.exists{_.isEmpty}) {
      dao.metricsManager ! IncrementMetric("snapshotCBAcceptQueryFailed")
    }

    for (
      cbOpt <- cbData;
      cbCache <- cbOpt;
      cb <- cbCache.checkpointBlock;
      tx <- cb.transactions
    ) {
      // TODO: Should really apply this to the N-1 snapshot instead of doing it directly
      // To allow consensus more time since the latest snapshot includes all data up to present, but this is simple for now
      tx.ledgerApplySnapshot()
      dao.transactionService.delete(Set(tx.hash))
      dao.metricsManager ! IncrementMetric("snapshotAppliedBalance")
    }
  }


}


class SnapshotTrigger(dao: DAO)
                     (implicit timeout: Timeout, executionContext: ExecutionContext) extends Actor with ActorLogging {

  implicit val sys: ActorSystem = context.system
  implicit val kp: KeyPair = dao.keyPair
  implicit val _dao: DAO = dao

  dao.heartbeatActor ! HeartbeatSubscribe

  def receive: Receive = active()

  def active(): Receive = {

    case InternalHeartbeat(round) =>

    case go: GenesisObservation =>

      // Dumb way to set these as active tips, won't pass a double validation but no big deal.
      dao.acceptGenesis(go)

    /*//      @deprecated
        case ConsensusRoundResult(checkpointBlock, roundHash: RoundHash[Checkpoint]) =>
          log.debug(s"handle checkpointBlock = $checkpointBlock")

        // handleCheckpoint(checkpointBlock, dao)*/

  }

}