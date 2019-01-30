package org.constellation.consensus

import java.nio.file.Path
import java.security.KeyPair

import cats.implicits._
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.{APIClient, ProductHash}

import scala.async.Async.{async, await}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

object EdgeProcessor {

  val logger = Logger(s"EdgeProcessor")

  def acceptCheckpoint(checkpointCacheData: CheckpointCacheData)(
      implicit dao: DAO): Unit = {

    if (checkpointCacheData.checkpointBlock.isEmpty) {
      dao.metricsManager ! IncrementMetric("acceptCheckpointCalledWithEmptyCB")
    } else {

      val cb = checkpointCacheData.checkpointBlock.get

      cb.storeSOE()

      val height = cb.calculateHeight()

      val fallbackHeight =
        if (height.isEmpty) checkpointCacheData.height else height

      if (fallbackHeight.isEmpty) {
        dao.metricsManager ! IncrementMetric("heightEmpty")
      } else {
        dao.metricsManager ! IncrementMetric("heightNonEmpty")
      }

      cb.checkpoint.edge.data.messages.foreach {
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
      Seq(TypedEdgeHash(tips.head.hash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.CheckpointHash)
      ),data = TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash)
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(observationEdge, soe, checkpointEdgeData)
    )

    CheckpointBlock(transactions, checkpointEdge)
  }



  case class SignatureRequest(checkpointBlock: CheckpointBlock, facilitators: Set[Id])
  case class  SignatureResponse(checkpointBlock: CheckpointBlock, facilitators: Set[Id], reRegister: Boolean = false)
  case class FinishedCheckpoint(checkpointCacheData: CheckpointCacheData, facilitators: Set[Id])
  case class FinishedCheckpointResponse(reRegister: Boolean = false)

  // TODO: Move to checkpoint formation actor
  def formCheckpoint(messages: Seq[ChannelMessage] = Seq())(
      implicit dao: DAO): Unit = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    val maybeTransactions =
      dao.threadSafeTXMemPool.pull(dao.minCheckpointFormationThreshold)

    dao.metricsManager ! IncrementMetric("attemptFormCheckpointCalls")

    if (maybeTransactions.isEmpty) {
      dao.metricsManager ! IncrementMetric(
        "attemptFormCheckpointInsufficientTX")
    }

    maybeTransactions.foreach { transactions =>
      val maybeTips = dao.threadSafeTipService.pull()
      if (maybeTips.isEmpty) {
        dao.metricsManager ! IncrementMetric(
          "attemptFormCheckpointInsufficientTipsOrFacilitators")
      }

      maybeTips.foreach {
        case (tipSOE, facils) =>
          val checkpointBlock =
            createCheckpointBlock(transactions, tipSOE, messages)(dao.keyPair)
          dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

          val finalFacilitators = facils.keySet

          val t = facils.mapValues { data =>
            async {
              val resp = await(
                data.client.postNonBlocking[Option[SignatureResponse]](
                  "request/signature",
                  SignatureRequest(checkpointBlock, finalFacilitators + dao.id),
                  10.seconds
                ))

              val sigRespOpt = resp
              sigRespOpt.foreach { sr =>
                if (sr.reRegister) {
                  // PeerManager.attemptRegisterPeer() TODO : Finish
                }
              }

              sigRespOpt.map { sigResp =>
                sigResp.checkpointBlock
              }
            }
          }

          // For now, blocking on these calls.
          // Future improvement is to make the rest of this async too.
          val cpBlocksOptList = Future.sequence(t.values).get().toList

          val cpBlocksOpt = cpBlocksOptList.sequence

          if (cpBlocksOpt.isEmpty) {
            // log debug map
            val m = t.mapValues(_.get())
            logger.warn(s"At least one signature request failed", m)

            dao.metricsManager ! IncrementMetric(
              "formCheckpointSignatureResponseEmpty")
          }
          cpBlocksOpt.foreach { cpBlocks =>
            val finalCB = cpBlocks.reduce(_ + _) + checkpointBlock
            if (finalCB.simpleValidation()) {
              val cache =
                CheckpointCacheData(
                  Some(finalCB),
                  height = finalCB.calculateHeight()
                )

              dao.threadSafeTipService.accept(cache)

              dao.peerInfo.values.foreach { peer =>
                wrapFutureWithMetric(
                  peer.client.post(
                    s"finished/checkpoint",
                    FinishedCheckpoint(cache, finalFacilitators),
                    timeout = 20.seconds
                  ),
                  "finishedCheckpointBroadcast",
                )
              }
            }
          }

          // Cleanup locks

          messages.foreach { m =>
            dao.threadSafeMessageMemPool
              .activeChannels(m.signedMessageData.data.channelId)
              .release()
          }

      }
    }
    dao.blockFormationInProgress = false
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

  def simpleResolveCheckpoint(hash: String)(implicit dao: DAO): Future[Boolean] = {

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


    for (
      cbOpt <- cbData;
      cbCache <- cbOpt;
      cb <- cbCache.checkpointBlock;
      message <- cb.checkpoint.edge. data.messages
    ) {
      dao.messageService.update(message.signedMessageData.signatures.hash,
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
