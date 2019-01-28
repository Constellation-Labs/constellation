package org.constellation.consensus

import java.nio.file.Path
import java.security.KeyPair
import java.util.concurrent.TimeUnit
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.DAO
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.{APIClient, ProductHash}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

// doc
object EdgeProcessor {

  // doc
  case class HandleTransaction(tx: Transaction)

  // doc
  case class HandleCheckpoint(checkpointBlock: CheckpointBlock)

  // doc
  case class HandleSignatureRequest(checkpointBlock: CheckpointBlock)

  val logger = Logger(s"EdgeProcessor")
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  // doc
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

      cb.checkpoint.edge.resolvedObservationEdge.data.get.messages.foreach { m =>
        dao.messageService.put(m.signedMessageData.data.channelId, ChannelMessageMetadata(m, Some(cb.baseHash)))
        dao.messageService.put(m.signedMessageData.signatures.hash, ChannelMessageMetadata(m, Some(cb.baseHash)))
        dao.metricsManager ! IncrementMetric("messageAccepted")
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

    } // end else

  } // end acceptCheckpoint

  /** Main transaction processing cell.
    * This is triggered upon external receipt of a transaction. Assume that the transaction being processed
    * came from a peer, not an internal operation.
    *
    * @param tx               ... Transaction with all data
    * @param dao              ... Data access object for referencing memPool and other actors
    * @param executionContext ... Threadpool to execute transaction processing against. Should be separate
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

  // doc
  def reportInvalidTransaction(dao: DAO, t: TransactionValidationStatus): Unit = {
    dao.metricsManager ! IncrementMetric("invalidTransactions")
    if (t.isDuplicateHash) {
      dao.metricsManager ! IncrementMetric("hashDuplicateTransactions")
    }
    if (!t.sufficientBalance) {
      dao.metricsManager ! IncrementMetric("insufficientBalanceTransactions")
    }
  }

  // doc
  case class CreateCheckpointEdgeResponse(
                                           checkpointEdge: CheckpointEdge,
                                           transactionsUsed: Set[String],
                                           // filteredValidationTips: Seq[SignedObservationEdge],
                                           updatedTransactionMemPoolThresholdMet: Set[String]
                                         )

  // doc
  def createCheckpointBlock(
                             transactions: Seq[Transaction],
                             tips: Seq[SignedObservationEdge],
                             messages: Seq[ChannelMessage] = Seq()
                           )(implicit keyPair: KeyPair): CheckpointBlock = {

    val checkpointEdgeData = CheckpointEdgeData(transactions.map {
      _.hash
    }.sorted, messages)

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

  // doc
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

  // doc
  case class SignatureRequest(checkpointBlock: CheckpointBlock, facilitators: Set[Id])

  // doc
  case class SignatureResponseWrapper(signatureResponse: Option[SignatureResponse] = None)

  // doc
  case class SignatureResponse(checkpointBlock: CheckpointBlock, facilitators: Set[Id], reRegister: Boolean = false)

  // doc
  case class FinishedCheckpoint(checkpointCacheData: CheckpointCacheData, facilitators: Set[Id])

  // doc
  case class FinishedCheckpointResponse(reRegister: Boolean = false)

  // TODO: Move to checkpoint formation actor

  // doc
  def formCheckpoint(messages: Seq[ChannelMessage] = Seq())(implicit dao: DAO): Unit = {

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

        val checkpointBlock = createCheckpointBlock(transactions, tipSOE, messages)(dao.keyPair)
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

                val result = Try {
                  v.unsafeBody.x[Option[SignatureResponseWrapper]].flatMap {
                    _.signatureResponse
                  }
                }
                if (result.isFailure) {
                  logger.error(s"Json response parsing error code: ${v.code} body: ${v.body}")
                  dao.metricsManager ! IncrementMetric(s"signatureResponseCode_${v.code}")
                }
                result.get
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

                val responseValues = withValue.values.map {
                  _.get
                }

                responseValues.foreach { sr =>

                  if (sr.reRegister) {
                    // PeerManager.attemptRegisterPeer() TODO : Finish

                  }

                }

                val blocks = responseValues.map {
                  _.checkpointBlock
                }

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
                      futureTryWithTimeoutMetric(
                        client.client.postSync(s"finished/checkpoint", FinishedCheckpoint(cache, finalFacilitators)),
                        "finishedCheckpointBroadcast",
                        timeoutSeconds = 20
                      )
                    }

                  } // end else
                } // end else
              } // end else
            } // end else
          } // end else
        } // end else

        // Cleanup locks

        messages.foreach { m =>
          dao.threadSafeMessageMemPool.activeChannels(m.signedMessageData.data.channelId).release()
        }

      } // end maybeTips for loop

    } // end maybeTransactions for loop

    dao.blockFormationInProgress = false

  } // end formCheckpoint

  // Temporary for testing join/leave logic.

  // doc
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
    /*
    dao.peerManager ! APIBroadcast(
      _.post(s"response/signature", SignatureResponse(updated, sr.facilitators)),
      peerSubset = Set(replyTo)
    )
    */
    // } else None
  }

  // doc
  def simpleResolveCheckpoint(hash: String)(implicit dao: DAO): Boolean = {

    var activePeer = dao.peerInfo.values.head.client
    var remainingPeers: Seq[APIClient] = dao.peerInfo.values.map {
      _.client
    }.filterNot(_ == activePeer).toSeq

    var done = false

    while (!done && remainingPeers.nonEmpty) {

      // TODO: Refactor all the error handling on these to include proper status codes etc.
      // See formCheckpoint for better example of error handling
      val res = tryWithMetric(
        {
          activePeer.getBlocking[Option[CheckpointCacheData]]("checkpoint/" + hash, timeout = 10.seconds)
        },
        "downloadCheckpoint"
      )

      done = res.toOption.exists(_.nonEmpty)

      if (done) {
        val x = res.get.get
        if (!dao.checkpointService.contains(x.checkpointBlock.get.baseHash)) {
          dao.metricsManager ! IncrementMetric("resolveAcceptCBCall")
          acceptWithResolveAttempt(x)
        }
      } else {
        if (remainingPeers.nonEmpty) {
          dao.metricsManager ! IncrementMetric("resolvePeerIncrement")
          activePeer = remainingPeers.head
          remainingPeers = remainingPeers.filterNot(_ == activePeer)
        }
      }
    }
    done

  } // end simpleResolveCheckpoint

  // doc
  def acceptWithResolveAttempt(checkpointCacheData: CheckpointCacheData)(implicit dao: DAO): Unit = {

    dao.threadSafeTipService.accept(checkpointCacheData)
    val block = checkpointCacheData.checkpointBlock.get
    val parents = block.parentSOEBaseHashes
    val parentExists = parents.map { h => h -> dao.checkpointService.contains(h) }
    if (parentExists.forall(_._2)) {
      dao.metricsManager ! IncrementMetric("resolveFinishedCheckpointParentsPresent")
    } else {
      dao.metricsManager ! IncrementMetric("resolveFinishedCheckpointParentMissing")
      parentExists.filterNot(_._2).foreach {
        case (h, _) =>
          futureTryWithTimeoutMetric(
            simpleResolveCheckpoint(h),
            "resolveCheckpoint",
            timeoutSeconds = 30
          )(dao.edgeExecutionContext, dao)
      }

    }

  }

  // doc
  def handleFinishedCheckpoint(fc: FinishedCheckpoint)(implicit dao: DAO): Future[Try[Any]] = {
    futureTryWithTimeoutMetric(
      if (dao.nodeState == NodeState.DownloadCompleteAwaitingFinalSync) {
        dao.threadSafeTipService.syncBufferAccept(fc.checkpointCacheData)
        Future.successful(Unit)
      } else if (dao.nodeState == NodeState.Ready) {
        if (fc.checkpointCacheData.checkpointBlock.exists {
          _.simpleValidation()
        }) {
          acceptWithResolveAttempt(fc.checkpointCacheData)
        } else Future.successful(Unit)
      } else Future.successful(Unit)
      , "handleFinishedCheckpoint"
    )(dao.finishedExecutionContext, dao)
  }

} // end EdgeProcessor object

// doc
case class TipData(checkpointBlock: CheckpointBlock, numUses: Int)

// doc
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

// doc
case class Snapshot(lastSnapshot: String, checkpointBlocks: Seq[String]) extends ProductHash

// doc
case class StoredSnapshot(snapshot: Snapshot, checkpointCache: Seq[CheckpointCacheData])

// doc
case class DownloadComplete(latestSnapshot: Snapshot)

import java.nio.file.{Files, Paths}

// doc
object Snapshot {

  // doc
  def writeSnapshot(snapshot: StoredSnapshot)(implicit dao: DAO): Try[Path] = {
    tryWithMetric({
      val serialized = KryoSerializer.serializeAnyRef(snapshot)
      Files.write(Paths.get(dao.snapshotPath.pathAsString, snapshot.snapshot.hash), serialized)
      //File(dao.snapshotPath, snapshot.snapshot.hash).writeByteArray(serialized)
    },
      "writeSnapshot"
    )
  }

  // doc
  def loadSnapshot(snapshotHash: String)(implicit dao: DAO): Try[StoredSnapshot] = {
    tryWithMetric({
      KryoSerializer.deserializeCast[StoredSnapshot] {
        val byteArray = Files.readAllBytes(Paths.get(dao.snapshotPath.pathAsString, snapshotHash))
        //   val f = File(dao.snapshotPath, snapshotHash)
        byteArray
      }
    },
      "loadSnapshot"
    )
  }

  // doc
  def loadSnapshotBytes(snapshotHash: String)(implicit dao: DAO): Try[Array[Byte]] = {
    tryWithMetric({
      {
        val byteArray = Files.readAllBytes(Paths.get(dao.snapshotPath.pathAsString, snapshotHash))
        //   val f = File(dao.snapshotPath, snapshotHash)
        byteArray
      }
    },
      "loadSnapshot"
    )
  }

  // doc
  def snapshotHashes()(implicit dao: DAO): List[String] = {
    dao.snapshotPath.toJava.listFiles().map {
      _.getName
    }.toList
  }

  // doc
  def findLatestMessageWithSnapshotHash(
                                         depth: Int,
                                         lastMessage: Option[ChannelMessageMetadata],
                                         maxDepth: Int = 10
                                       )(implicit dao: DAO): Option[ChannelMessageMetadata] = {

    // doc
    def findLatestMessageWithSnapshotHashInner(
                                                depth: Int,
                                                lastMessage: Option[ChannelMessageMetadata]
                                              ): Option[ChannelMessageMetadata] = {
      if (depth > maxDepth) None
      else {
        lastMessage.flatMap { m =>
          if (m.snapshotHash.nonEmpty) Some(m)
          else {
            findLatestMessageWithSnapshotHashInner(
              depth + 1,
              dao.messageService.get(m.channelMessage.signedMessageData.data.previousMessageDataHash)
            )
          }
        }
      }
    }

    findLatestMessageWithSnapshotHashInner(depth, lastMessage)
  }

  // doc
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

  // doc
  def acceptSnapshot(snapshot: Snapshot)(implicit dao: DAO): Unit = {
    // dao.dbActor.putSnapshot(snapshot.hash, snapshot)
    val cbData = snapshot.checkpointBlocks.map {
      dao.checkpointService.get
    }

    if (cbData.exists {
      _.isEmpty
    }) {
      dao.metricsManager ! IncrementMetric("snapshotCBAcceptQueryFailed")
    }

    for (
      cbOpt <- cbData;
      cbCache <- cbOpt;
      cb <- cbCache.checkpointBlock;
      data <- cb.checkpoint.edge.resolvedObservationEdge.data;
      message <- data.messages
    ) {
      dao.messageService.update(message.signedMessageData.signatures.hash,
        _.copy(snapshotHash = Some(snapshot.hash)),
        ChannelMessageMetadata(message, Some(cb.baseHash), Some(snapshot.hash))
      )
      dao.metricsManager ! IncrementMetric("messageSnapshotHashUpdated")
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

  } // end acceptSnapshot

} // end Snapshot object
