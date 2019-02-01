package org.constellation.consensus

import java.nio.file.Path
import java.security.KeyPair

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.Validation.EnrichedFuture
import org.constellation.util.{APIClient, ProductHash}

import scala.async.Async.{async, await}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

object EdgeProcessor extends StrictLogging {

  case class HandleTransaction(tx: Transaction)
  case class HandleCheckpoint(checkpointBlock: CheckpointBlock)
  case class HandleSignatureRequest(checkpointBlock: CheckpointBlock)

  def acceptCheckpoint(checkpointCacheData: CheckpointCacheData)(
      implicit dao: DAO): Unit = {

    if (checkpointCacheData.checkpointBlock.isEmpty) {
      dao.metricsManager ! IncrementMetric("acceptCheckpointCalledWithEmptyCB")
    } else {

      val cb = checkpointCacheData.checkpointBlock.get

      val height = cb.calculateHeight()

      val fallbackHeight =
        if (height.isEmpty) checkpointCacheData.height else height

      if (fallbackHeight.isEmpty) {
        dao.metricsManager ! IncrementMetric("heightEmpty")
      } else {
        dao.metricsManager ! IncrementMetric("heightNonEmpty")
      }

      cb.checkpoint.edge.resolvedObservationEdge.data.get.messages.foreach {
        m =>
          dao.messageService.put(m.signedMessageData.data.channelId,
                                 ChannelMessageMetadata(m, Some(cb.baseHash)))
          dao.messageService.put(m.signedMessageData.signatures.hash,
                                 ChannelMessageMetadata(m, Some(cb.baseHash)))
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
      tx: Transaction,
      skipValidation: Boolean = true
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

      dao.metricsManager ! UpdateMetric(
        "transactionMemPool",
        dao.threadSafeTXMemPool.unsafeCount.toString)
      dao.metricsManager ! IncrementMetric("transactionValidMessages")
      formCheckpoint().getTry(60) // TODO: Send to checkpoint formation actor instead
    } else {
      dao.metricsManager ! IncrementMetric(
        "transactionValidMemPoolDuplicateMessages")
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

  def reportInvalidTransaction(dao: DAO,
                               t: TransactionValidationStatus): Unit = {
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

  def createCheckpointBlock(
      transactions: Seq[Transaction],
      tips: Seq[SignedObservationEdge],
      messages: Seq[ChannelMessage] = Seq()
  )(implicit keyPair: KeyPair): CheckpointBlock = {

    val checkpointEdgeData =
      CheckpointEdgeData(transactions.map { _.hash }.sorted, messages)

    val observationEdge = ObservationEdge(
      TypedEdgeHash(tips.head.hash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.CheckpointHash),
      data = Some(
        TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(
        observationEdge,
        soe,
        ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData)))
    )

    CheckpointBlock(transactions, checkpointEdge)
  }

  def createCheckpointEdgeProposal(
      transactionMemPoolThresholdMet: Set[String],
      minCheckpointFormationThreshold: Int,
      tips: Seq[SignedObservationEdge],
  )(implicit keyPair: KeyPair): CreateCheckpointEdgeResponse = {

    val transactionsUsed =
      transactionMemPoolThresholdMet.take(minCheckpointFormationThreshold)
    val updatedTransactionMemPoolThresholdMet = transactionMemPoolThresholdMet -- transactionsUsed

    val checkpointEdgeData = CheckpointEdgeData(transactionsUsed.toSeq.sorted)

    //val tips = validationTips.take(2)
    //val filteredValidationTips = validationTips.filterNot(tips.contains)

    val observationEdge = ObservationEdge(
      TypedEdgeHash(tips.head.hash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.CheckpointHash),
      data = Some(
        TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(
        observationEdge,
        soe,
        ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData))))

    CreateCheckpointEdgeResponse(checkpointEdge,
                                 transactionsUsed,
                                 //filteredValidationTips,
                                 updatedTransactionMemPoolThresholdMet)
  }
  // TODO: Send facilitator selection data (i.e. criteria) as well for verification

  case class SignatureRequest(checkpointBlock: CheckpointBlock,
                              facilitators: Set[Id])
  case class SignatureResponse(checkpointBlock: CheckpointBlock,
                               facilitators: Set[Id],
                               reRegister: Boolean = false)
  case class FinishedCheckpoint(checkpointCacheData: CheckpointCacheData,
                                facilitators: Set[Id])
  case class FinishedCheckpointResponse(reRegister: Boolean = false)

  // TODO: Move to checkpoint formation actor
  def formCheckpoint(messages: Seq[ChannelMessage] = Seq())(
      implicit dao: DAO
  ) = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    val maybeTransactions =
      dao.threadSafeTXMemPool.pull(dao.minCheckpointFormationThreshold)

    dao.metricsManager ! IncrementMetric("attemptFormCheckpointCalls")

    if (maybeTransactions.isEmpty) {
      dao.metricsManager ! IncrementMetric(
        "attemptFormCheckpointInsufficientTX")
    }

    def requestBlockSignature(
        checkpointBlock: CheckpointBlock,
        finalFacilitators: Set[
          Id
        ],
        data: PeerData
    ) = {
      async {
        val sigResp = await(
          data.client.postNonBlocking[SignatureResponse](
            "request/signature",
            SignatureRequest(checkpointBlock, finalFacilitators + dao.id),
            15.seconds
          )
        )

        if (sigResp.reRegister) {
          // PeerManager.attemptRegisterPeer() TODO : Finish
        }

        sigResp.checkpointBlock
      }
    }
    def processSignedBlock(
        cache: CheckpointCacheData,
        finalFacilitators: Set[
          Id
        ]
    ) = {

      val responses = dao.peerInfo.values.toList.map { peer =>
        wrapFutureWithMetric(
          peer.client.postNonBlocking[Option[FinishedCheckpointResponse]](
            "finished/checkpoint",
            FinishedCheckpoint(cache, finalFacilitators),
            timeout = 20.seconds
          ),
          "finishedCheckpointBroadcast",
        )
      }

      responses.traverse(_.toValidatedNel).map(_.sequence)
    }

    val result = maybeTransactions.flatMap { transactions =>
      val maybeTips = dao.threadSafeTipService.pull()
      if (maybeTips.isEmpty) {
        dao.metricsManager ! IncrementMetric(
          "attemptFormCheckpointInsufficientTipsOrFacilitators")
      }

      maybeTips.map {
        case (tipSOE, facils) =>
          val checkpointBlock =
            createCheckpointBlock(transactions, tipSOE, messages)(dao.keyPair)
          dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

          val finalFacilitators = facils.keySet

          val blockResults = facils.values.toList
            .traverse { peerData =>
              requestBlockSignature(checkpointBlock,
                                    finalFacilitators,
                                    peerData).toValidatedNel
            }
            .flatMap { blockResultsList =>
              blockResultsList.sequence
                .map { cpBlocks =>
                  cpBlocks.reduce(_ + _) + checkpointBlock
                }
                .ensure(NonEmptyList.one(new Throwable(
                  "Invalid CheckpointBlock")))(_.simpleValidation())
                .traverse { finalCB =>
                  val cache = CheckpointCacheData(finalCB.some,
                                                  height =
                                                    finalCB.calculateHeight())
                  dao.threadSafeTipService.accept(cache)
                  processSignedBlock(cache, finalFacilitators)
                }
                .map { _.andThen(identity) }
            }

          wrapFutureWithMetric(blockResults, "checkpointBlockFormation")

          blockResults.foreach {
            case Valid(_) =>
            case Invalid(failures) =>
              failures.toList.foreach { e =>
                dao.metricsManager ! IncrementMetric(
                  "formCheckpointSignatureResponseError"
                )
                logger.warn("Failure gathering signature", e)
              }

          }

          // using transform kind of like a finally for Future.
          // I want to ensure the locks get cleaned up
          blockResults.transform { res =>
            // Cleanup locks

            messages.foreach { m =>
              dao.threadSafeMessageMemPool
                .activeChannels(m.signedMessageData.data.channelId)
                .release()
            }
            res.map(_ => true)
          }

      }
    }
    dao.blockFormationInProgress = false
    result.sequence
  }

  // Temporary for testing join/leave logic.
  def handleSignatureRequest(sr: SignatureRequest)(
      implicit dao: DAO): SignatureResponse = {
    //if (sr.facilitators.contains(dao.id)) {
    // val replyTo = sr.checkpointBlock.witnessIds.head
    val updated = if (sr.checkpointBlock.simpleValidation()) {
      sr.checkpointBlock.plus(dao.keyPair)
    } else {
      sr.checkpointBlock
    }
    SignatureResponse(updated, sr.facilitators)
    /*dao.peerManager ! APIBroadcast(
      _.post(s"response/signature", SignatureResponse(updated, sr.facilitators)),
      peerSubset = Set(replyTo)
    )*/
    // } else None
  }

  def simpleResolveCheckpoint(hash: String)(
      implicit dao: DAO): Future[Boolean] = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    def innerResolve(peers: List[APIClient])(
        implicit ec: ExecutionContext): Future[CheckpointCacheData] = {
      peers match {
        case activePeer :: rest =>
          val resp = activePeer.getNonBlocking[Option[CheckpointCacheData]](
            s"checkpoint/$hash",
            timeout = 10.seconds)
          resp.flatMap {
            case Some(ccd) => Future.successful(ccd)
            case None =>
              dao.metricsManager ! IncrementMetric("resolvePeerIncrement")
              innerResolve(rest)
          }

        case Nil =>
          Future.failed(
            new RuntimeException(s"Unable to resolve checkpoint hash $hash"))

      }
    }

    val resolved = wrapFutureWithMetric(
      innerResolve(dao.peerInfo.values.map(_.client).toList)(
        dao.edgeExecutionContext),
      "simpleResolveCheckpoint"
    )

    resolved.map { checkpointCacheData =>
      if (!dao.checkpointService.contains(
            checkpointCacheData.checkpointBlock.get.baseHash)) {
        dao.metricsManager ! IncrementMetric("resolveAcceptCBCall")
        acceptWithResolveAttempt(checkpointCacheData)
      }
      true
    }

  }

  def acceptWithResolveAttempt(checkpointCacheData: CheckpointCacheData)(
      implicit dao: DAO): Unit = {

    dao.threadSafeTipService.accept(checkpointCacheData)
    val block = checkpointCacheData.checkpointBlock.get
    val parents = block.parentSOEBaseHashes
    val parentExists = parents.map { h =>
      h -> dao.checkpointService.contains(h)
    }
    if (parentExists.forall(_._2)) {
      dao.metricsManager ! IncrementMetric(
        "resolveFinishedCheckpointParentsPresent")
    } else {
      dao.metricsManager ! IncrementMetric(
        "resolveFinishedCheckpointParentMissing")
      parentExists.filterNot(_._2).foreach {
        case (h, _) =>
          wrapFutureWithMetric(
            simpleResolveCheckpoint(h),
            "resolveCheckpoint"
          )(dao, dao.edgeExecutionContext)
      }

    }

  }

  def handleFinishedCheckpoint(fc: FinishedCheckpoint)(implicit dao: DAO) = {
    futureTryWithTimeoutMetric(
      if (dao.nodeState == NodeState.DownloadCompleteAwaitingFinalSync) {
        dao.threadSafeTipService.syncBufferAccept(fc.checkpointCacheData)
      } else if (dao.nodeState == NodeState.Ready) {
        if (fc.checkpointCacheData.checkpointBlock.exists {
              _.simpleValidation()
            }) {
          acceptWithResolveAttempt(fc.checkpointCacheData)
        }
      },
      "handleFinishedCheckpoint"
    )(dao.finishedExecutionContext, dao)
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

case class Snapshot(lastSnapshot: String, checkpointBlocks: Seq[String])
    extends ProductHash
case class StoredSnapshot(snapshot: Snapshot,
                          checkpointCache: Seq[CheckpointCacheData])

case class DownloadComplete(latestSnapshot: Snapshot)

import java.nio.file.{Files, Paths}

object Snapshot {

  def writeSnapshot(snapshot: StoredSnapshot)(implicit dao: DAO): Try[Path] = {
    tryWithMetric(
      {
        val serialized = KryoSerializer.serializeAnyRef(snapshot)
        Files.write(
          Paths.get(dao.snapshotPath.pathAsString, snapshot.snapshot.hash),
          serialized)
        //File(dao.snapshotPath, snapshot.snapshot.hash).writeByteArray(serialized)
      },
      "writeSnapshot"
    )
  }

  def loadSnapshot(snapshotHash: String)(
      implicit dao: DAO): Try[StoredSnapshot] = {
    tryWithMetric(
      {
        KryoSerializer.deserializeCast[StoredSnapshot] {
          val byteArray = Files.readAllBytes(
            Paths.get(dao.snapshotPath.pathAsString, snapshotHash))
          //   val f = File(dao.snapshotPath, snapshotHash)
          byteArray
        }
      },
      "loadSnapshot"
    )
  }

  def loadSnapshotBytes(snapshotHash: String)(
      implicit dao: DAO): Try[Array[Byte]] = {
    tryWithMetric(
      {
        {
          val byteArray = Files.readAllBytes(
            Paths.get(dao.snapshotPath.pathAsString, snapshotHash))
          //   val f = File(dao.snapshotPath, snapshotHash)
          byteArray
        }
      },
      "loadSnapshot"
    )
  }

  def snapshotHashes()(implicit dao: DAO): List[String] = {
    dao.snapshotPath.toJava.listFiles().map { _.getName }.toList
  }

  def findLatestMessageWithSnapshotHash(
      depth: Int,
      lastMessage: Option[ChannelMessageMetadata],
      maxDepth: Int = 10
  )(implicit dao: DAO): Option[ChannelMessageMetadata] = {

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
              dao.messageService.get(
                m.channelMessage.signedMessageData.data.previousMessageDataHash)
            )
          }
        }
      }
    }
    findLatestMessageWithSnapshotHashInner(depth, lastMessage)
  }

  def triggerSnapshot(round: Long)(implicit dao: DAO): Unit = {
    // TODO: Refactor round into InternalHeartbeat
    if (round % dao.processingConfig.snapshotInterval == 0 && dao.nodeState == NodeState.Ready) {
      futureTryWithTimeoutMetric(
        dao.threadSafeTipService.attemptSnapshot(),
        "snapshotAttempt"
      )(dao.edgeExecutionContext, dao)
    }
  }

  val snapshotZero = Snapshot("", Seq())
  val snapshotZeroHash: String = Snapshot("", Seq()).hash

  def acceptSnapshot(snapshot: Snapshot)(implicit dao: DAO): Unit = {
    // dao.dbActor.putSnapshot(snapshot.hash, snapshot)
    val cbData = snapshot.checkpointBlocks.map { dao.checkpointService.get }

    if (cbData.exists { _.isEmpty }) {
      dao.metricsManager ! IncrementMetric("snapshotCBAcceptQueryFailed")
    }

    for (cbOpt <- cbData;
         cbCache <- cbOpt;
         cb <- cbCache.checkpointBlock;
         data <- cb.checkpoint.edge.resolvedObservationEdge.data;
         message <- data.messages) {
      dao.messageService.update(
        message.signedMessageData.signatures.hash,
        _.copy(snapshotHash = Some(snapshot.hash)),
        ChannelMessageMetadata(message, Some(cb.baseHash), Some(snapshot.hash))
      )
      dao.metricsManager ! IncrementMetric("messageSnapshotHashUpdated")
    }

    for (cbOpt <- cbData;
         cbCache <- cbOpt;
         cb <- cbCache.checkpointBlock;
         tx <- cb.transactions) {
      // TODO: Should really apply this to the N-1 snapshot instead of doing it directly
      // To allow consensus more time since the latest snapshot includes all data up to present, but this is simple for now
      tx.ledgerApplySnapshot()
      dao.transactionService.delete(Set(tx.hash))
      dao.metricsManager ! IncrementMetric("snapshotAppliedBalance")
    }
  }

}
