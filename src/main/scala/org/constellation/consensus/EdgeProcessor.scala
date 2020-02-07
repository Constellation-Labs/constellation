package org.constellation.consensus

import java.nio.file.NoSuchFileException

import better.files.File
import cats.data.Validated.{Invalid, Valid}
import cats.data.{EitherT, NonEmptyList}
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation.{getCCParams, _}
import org.constellation.domain.snapshotInfo.SnapshotInfoChunk
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.util.Validation.EnrichedFuture
import org.constellation.util._
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

import scala.async.Async.{async, await}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

case class CreateCheckpointEdgeResponse(
  checkpointEdge: CheckpointEdge,
  transactionsUsed: Set[String],
  // filteredValidationTips: Seq[SignedObservationEdge],
  updatedTransactionMemPoolThresholdMet: Set[String]
)

case class SignatureRequest(checkpointBlock: CheckpointBlock, facilitators: Set[Id])

case class SignatureResponse(signature: Option[HashSignature], reRegister: Boolean = false)
case class FinishedCheckpoint(checkpointCacheData: CheckpointCache, facilitators: Set[Id])

case class FinishedCheckpointResponse(isSuccess: Boolean = false)

object EdgeProcessor extends StrictLogging {

  private def requestBlockSignature(
    checkpointBlock: CheckpointBlock,
    finalFacilitators: Set[
      Id
    ],
    data: PeerData
  )(implicit dao: DAO, ec: ExecutionContext) =
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

      sigResp.signature
    }

  private def processSignedBlock(
    cache: CheckpointCache,
    finalFacilitators: Set[
      Id
    ]
  )(implicit dao: DAO, ec: ExecutionContext) = {

    val responses = dao.peerInfo.unsafeRunSync().values.toList.map { peer =>
      wrapFutureWithMetric(
        peer.client.postNonBlockingUnit(
          "finished/checkpoint",
          FinishedCheckpoint(cache, finalFacilitators),
          timeout = 8.seconds,
          Map(
            "ReplyTo" -> APIClient(dao.nodeConfig.hostName, dao.nodeConfig.peerHttpPort)(dao.backend)
              .base("finished/reply")
          )
        ),
        "finishedCheckpointBroadcast"
      )
    }

    responses.traverse(_.toValidatedNel).map(_.sequence)
  }

  def formCheckpoint(messages: Seq[ChannelMessage] = Seq())(
    implicit dao: DAO
  ) = {

    implicit val ec: ExecutionContextExecutor = ConstellationExecutionContext.bounded
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    val transactions = dao.transactionService
      .pullForConsensus(ConfigUtil.constellation.getInt("consensus.maxTransactionThreshold"))
      .map(_.map(_.transaction))
      .unsafeRunSync()

    dao.metrics.incrementMetric("attemptFormCheckpointCalls")

    if (transactions.isEmpty) {
      dao.metrics.incrementMetric("attemptFormCheckpointNoTX")
    }
    val readyFacilitators = dao.readyFacilitatorsAsync.unsafeRunSync()

    if (readyFacilitators.isEmpty) {
      dao.metrics.incrementMetric("attemptFormCheckpointInsufficientTipsOrFacilitators")
      if (dao.nodeConfig.isGenesisNode) {
        val maybeTips = dao.concurrentTipService.pull(Map.empty)(dao.metrics).unsafeRunSync()
        if (maybeTips.isEmpty) {
          dao.metrics.incrementMetric("attemptFormCheckpointNoGenesisTips")
        }

        maybeTips.foreach { pulledTip =>
          val checkpointBlock =
            CheckpointBlock.createCheckpointBlock(transactions, pulledTip.tipSoe.soe.map { soe =>
              TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash)
            }, messages)(dao.keyPair)

          val cache =
            CheckpointCache(
              checkpointBlock,
              height = dao.checkpointAcceptanceService.calculateHeight(checkpointBlock).unsafeRunSync()
            )

          dao.checkpointAcceptanceService.accept(cache).unsafeRunSync()
          dao.threadSafeMessageMemPool.release(messages)

        }
      }

    }

    val result = dao.concurrentTipService.pull(readyFacilitators)(dao.metrics).unsafeRunSync().map { pulledTip =>
      // Change to method on TipsReturned // abstract for reuse.
      val checkpointBlock = CheckpointBlock.createCheckpointBlock(transactions, pulledTip.tipSoe.soe.map { soe =>
        TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash)
      }, messages)(dao.keyPair)
      dao.metrics.incrementMetric("checkpointBlocksCreated")

      val finalFacilitators = pulledTip.peers.keySet

      val signatureResults = pulledTip.peers.values.toList.traverse { peerData =>
        requestBlockSignature(checkpointBlock, finalFacilitators, peerData).toValidatedNel
      }.flatMap { signatureResultList =>
        signatureResultList.sequence.map { signatures =>
          // Unsafe flatten -- revisit during consensus updates
          signatures.flatten.foldLeft(checkpointBlock) {
            case (cb, hs) =>
              cb.plus(hs)
          }
        }.ensure(NonEmptyList.one(new Throwable("Invalid CheckpointBlock")))(
            cb => dao.checkpointBlockValidator.simpleValidation(cb).unsafeRunSync().isValid
          )
          .traverse { finalCB =>
            val cache = CheckpointCache(finalCB)
            dao.checkpointAcceptanceService.accept(cache).unsafeRunSync()
            processSignedBlock(
              cache,
              finalFacilitators
            )
          }
      }

      wrapFutureWithMetric(signatureResults, "checkpointBlockFormation")

      signatureResults.foreach {
        case Valid(_) =>
        case Invalid(failures) =>
          failures.toList.foreach { e =>
            dao.metrics.incrementMetric(
              "formCheckpointSignatureResponseError"
            )
            logger.warn("Failure gathering signature", e)
          }

      }

      // using transform kind of like a finally for Future.
      // I want to ensure the locks get cleaned up
      signatureResults.transform { res =>
        // Cleanup locks

        dao.threadSafeMessageMemPool.release(messages)
        res.map(_ => true)
      }
    }
    result.sequence
  }

  def handleSignatureRequest(
    sr: SignatureRequest
  )(implicit dao: DAO): Future[Try[SignatureResponse]] =
    futureTryWithTimeoutMetric(
      {
        dao.metrics.incrementMetric("peerApiRXSignatureRequest")

        val hashes = sr.checkpointBlock.checkpoint.edge.data.hashes
        dao.metrics.incrementMetric(
          "signatureRequestAllHashesKnown_" + hashes.forall { h =>
            dao.transactionService.lookup(h).map(_.nonEmpty).unsafeRunSync()
          }
        )

        val updated = if (dao.checkpointBlockValidator.simpleValidation(sr.checkpointBlock).unsafeRunSync().isValid) {
          Some(hashSign(sr.checkpointBlock.baseHash, dao.keyPair))
        } else {
          None
        }
        SignatureResponse(updated)
      },
      "handleSignatureRequest"
    )(ConstellationExecutionContext.bounded, dao)

  def chunkSerialize[T](chunk: Seq[T], tag: String): Array[Byte] = {
    logger.debug(s"ChunkSerialize : $tag")
    KryoSerializer.serializeAnyRef(chunk)
  }

  def chunkDeSerialize[T](chunk: Array[Byte], tag: String): T = {
    logger.debug(s"ChunkDeSerialize : $tag")
    KryoSerializer.deserializeCast[T](chunk)
  }
}

case class TipData(checkpointBlock: CheckpointBlock, numUses: Int, height: Height)

case class SnapshotInfo(
  snapshot: StoredSnapshot,
  acceptedCBSinceSnapshot: Seq[String] = Seq(),
  acceptedCBSinceSnapshotCache: Seq[CheckpointCache] = Seq(),
  awaitingCbs: Set[CheckpointCache] = Set(),
  lastSnapshotHeight: Int = 0,
  snapshotHashes: Seq[String] = Seq(),
  addressCacheData: Map[String, AddressCacheData] = Map(),
  tips: Map[String, TipData] = Map(),
  snapshotCache: Seq[CheckpointCache] = Seq(),
  lastAcceptedTransactionRef: Map[String, LastTransactionRef] = Map()
) {
  import EdgeProcessor.chunkSerialize

  def toSnapshotInfoSer(info: SnapshotInfo = this, chunkSize: Int = 100): SnapshotInfoSer = //todo make chunk size config
    SnapshotInfoSer(
      Array(KryoSerializer.serialize[String](info.snapshot.snapshot.lastSnapshot)),
      info.snapshot.checkpointCache
        .grouped(chunkSize)
        .map(t => chunkSerialize(t, SnapshotInfoChunk.STORED_SNAPSHOT_CHECKPOINT_BLOCKS.name))
        .toArray,
      info.snapshot.snapshot.checkpointBlocks
        .grouped(chunkSize)
        .map(t => chunkSerialize(t, SnapshotInfoChunk.CHECKPOINT_BLOCKS.name))
        .toArray,
      info.snapshot.snapshot.publicReputation
        .grouped(chunkSize)
        .map(t => chunkSerialize(t.toSeq, SnapshotInfoChunk.PUBLIC_REPUTATION.name))
        .toArray,
      info.acceptedCBSinceSnapshot
        .grouped(chunkSize)
        .map(t => chunkSerialize(t, SnapshotInfoChunk.ACCEPTED_CBS_SINCE_SNAPSHOT.name))
        .toArray,
      info.acceptedCBSinceSnapshotCache
        .grouped(chunkSize)
        .map(t => chunkSerialize(t, SnapshotInfoChunk.ACCEPTED_CBS_SINCE_SNAPSHOT_CACHE.name))
        .toArray,
      info.awaitingCbs
        .grouped(chunkSize)
        .map(t => chunkSerialize(t.toSeq, SnapshotInfoChunk.AWAITING_CBS.name))
        .toArray,
      Array(KryoSerializer.serialize[Int](info.lastSnapshotHeight)),
      info.snapshotHashes
        .grouped(chunkSize)
        .map(t => chunkSerialize(t, SnapshotInfoChunk.SNAPSHOT_HASHES.name))
        .toArray,
      info.addressCacheData.toSeq
        .grouped(chunkSize)
        .map(partitionMap => chunkSerialize(partitionMap, SnapshotInfoChunk.ADDRESS_CACHE_DATA.name))
        .toArray,
      info.tips
        .grouped(chunkSize)
        .map(partitionMap => chunkSerialize(partitionMap.toSeq, SnapshotInfoChunk.TIPS.name))
        .toArray,
      info.snapshotCache.grouped(chunkSize).map(t => chunkSerialize(t, SnapshotInfoChunk.SNAPSHOT_CACHE.name)).toArray,
      info.lastAcceptedTransactionRef
        .grouped(chunkSize)
        .map(partitionMap => chunkSerialize(partitionMap.toSeq, SnapshotInfoChunk.LAST_ACCEPTED_TX_REF.name))
        .toArray
    )
}

case class SnapshotInfoSer(
  snapshot: Array[Array[Byte]],
  storedSnapshotCheckpointBlocks: Array[Array[Byte]],
  snapshotCheckpointBlocks: Array[Array[Byte]],
  snapshotPublicReputation: Array[Array[Byte]],
  acceptedCBSinceSnapshot: Array[Array[Byte]],
  acceptedCBSinceSnapshotCache: Array[Array[Byte]],
  awaitingCbs: Array[Array[Byte]],
  lastSnapshotHeight: Array[Array[Byte]],
  snapshotHashes: Array[Array[Byte]],
  addressCacheData: Array[Array[Byte]],
  tips: Array[Array[Byte]],
  snapshotCache: Array[Array[Byte]],
  lastAcceptedTransactionRef: Array[Array[Byte]]
) {
  import EdgeProcessor.chunkDeSerialize

  def toSnapshotInfo(info: SnapshotInfoSer = this): SnapshotInfo = {
    val lastSnapshot = info.snapshot.map(KryoSerializer.deserializeCast[String]).head
    val snapshotCheckpointBlocks =
      info.snapshotCheckpointBlocks.toSeq
        .flatMap(chunkDeSerialize[Seq[String]](_, SnapshotInfoChunk.CHECKPOINT_BLOCKS.name))
    val snapshotPublicReputation =
      info.snapshotPublicReputation.toSeq
        .flatMap(chunkDeSerialize[Seq[(Id, Double)]](_, SnapshotInfoChunk.PUBLIC_REPUTATION.name))
        .toMap
    val storedSnapshotCheckpointBlocks = 
      info.storedSnapshotCheckpointBlocks.toSeq
        .flatMap(chunkDeSerialize[Seq[CheckpointCache]](_, SnapshotInfoChunk.STORED_SNAPSHOT_CHECKPOINT_BLOCKS.name))

    SnapshotInfo(
      StoredSnapshot(Snapshot(lastSnapshot, snapshotCheckpointBlocks, snapshotPublicReputation), storedSnapshotCheckpointBlocks),
      info.acceptedCBSinceSnapshot.toSeq
        .flatMap(chunkDeSerialize[Seq[String]](_, SnapshotInfoChunk.ACCEPTED_CBS_SINCE_SNAPSHOT.name)),
      info.acceptedCBSinceSnapshotCache.toSeq
        .flatMap(chunkDeSerialize[Seq[CheckpointCache]](_, SnapshotInfoChunk.ACCEPTED_CBS_SINCE_SNAPSHOT_CACHE.name)),
      info.awaitingCbs.toSet
        .flatMap(chunkDeSerialize[Set[CheckpointCache]](_, SnapshotInfoChunk.AWAITING_CBS.name)),
      info.lastSnapshotHeight.map(KryoSerializer.deserializeCast[Int]).head,
      info.snapshotHashes.toSeq.flatMap(chunkDeSerialize[Seq[String]](_, SnapshotInfoChunk.SNAPSHOT_HASHES.name)),
      info.addressCacheData.toSeq
        .flatMap(chunkDeSerialize[Seq[(String, AddressCacheData)]](_, SnapshotInfoChunk.ADDRESS_CACHE_DATA.name))
        .toMap,
      info.tips.toSeq.flatMap(chunkDeSerialize[Seq[(String, TipData)]](_, SnapshotInfoChunk.TIPS.name)).toMap,
      info.snapshotCache.toSeq
        .flatMap(chunkDeSerialize[Seq[CheckpointCache]](_, SnapshotInfoChunk.SNAPSHOT_CACHE.name)),
      info.lastAcceptedTransactionRef.toSeq
        .flatMap(chunkDeSerialize[Seq[(String, LastTransactionRef)]](_, SnapshotInfoChunk.LAST_ACCEPTED_TX_REF.name))
        .toMap
    )
  }

  def write[T](
    writerProc: (Seq[(String, Array[Byte])], String) => T
  )(infoSer: SnapshotInfoSer = this, basePath: String): T =
    writerProc(getInfoSerPartsPlan(infoSer), basePath)

  def writeLocal(infoSer: SnapshotInfoSer = this, basePath: String = "rollback_data/snapshot_info/"): Unit =
    write[Unit](localWriterProc)(infoSer, basePath)

  private def getInfoSerPartsPlan(infoSer: SnapshotInfoSer = this) = {
    val infoSerParts = getCCParams(infoSer).asInstanceOf[List[(String, Array[Array[Byte]])]]
    infoSerParts.flatMap {
      case (k, parts) =>
        parts.zipWithIndex.map {
          case (part, idx) => (s"$k-$idx", part)
        }
    }
  }

  private def localWriterProc(plan: Seq[(String, Array[Byte])], basePath: String): Unit =
    plan.foreach {
      case (path, part) => File(basePath, path).writeByteArray(part)
    }
}

case object GetMemPool

case class Snapshot(lastSnapshot: String, checkpointBlocks: Seq[String], publicReputation: Map[Id, Double])
    extends Signable

case class StoredSnapshot(snapshot: Snapshot, checkpointCache: Seq[CheckpointCache]) {

  def height: Long =
    checkpointCache.toList
      .maxBy(_.height.map(_.min).getOrElse(0L))
      .height
      .map(_.min)
      .getOrElse(0L)
}

case class DownloadComplete(latestSnapshot: Snapshot)

object Snapshot extends StrictLogging {

  def writeSnapshot[F[_]: Concurrent](
    storedSnapshot: StoredSnapshot
  )(implicit dao: DAO, C: ContextShift[F]): EitherT[F, Throwable, Unit] =
    for {
      serialized <- EitherT(Sync[F].delay(KryoSerializer.serializeAnyRef(storedSnapshot)).attempt)
      write <- EitherT(
        C.evalOn(ConstellationExecutionContext.unbounded)(writeSnapshot(storedSnapshot, serialized).value)
      )
    } yield write

  private def writeSnapshot[F[_]: Concurrent](
    storedSnapshot: StoredSnapshot,
    serialized: Array[Byte],
    trialNumber: Int = 0
  )(
    implicit dao: DAO,
    C: ContextShift[F]
  ): EitherT[F, Throwable, Unit] =
    trialNumber match {
      case x if x >= 3 => EitherT.leftT[F, Unit](new Throwable(s"Unable to write snapshot"))
      case _ =>
        LiftIO[F].liftIO(isOverDiskCapacity(serialized.length)).attemptT.flatMap { isOver =>
          if (isOver) {
            logger.warn(s"removeOldSnapshots in writeSnapshot")
            removeOldSnapshots().attemptT >> writeSnapshot(storedSnapshot, serialized, trialNumber + 1)
          } else {
            withMetric(
              LiftIO[F].liftIO {
                dao.snapshotStorage
                  .writeSnapshot(storedSnapshot.snapshot.hash, serialized)
                  .value
                  .flatMap(IO.fromEither)
              },
              "writeSnapshot"
            ).attemptT
          }
        }
    }

  def removeOldSnapshots[F[_]: Concurrent]()(implicit dao: DAO, C: ContextShift[F]): F[Unit] =
    for {
      hashes <- LiftIO[F].liftIO(dao.snapshotBroadcastService.getRecentSnapshots().map(_.map(_.hash)))
      diff <- LiftIO[F].liftIO(dao.snapshotStorage.getSnapshotHashes).map(_.toList.diff(hashes))
      _ <- removeSnapshots(diff)
    } yield ()

  def removeSnapshots[F[_]: Concurrent](
    snapshots: List[String]
  )(implicit dao: DAO, C: ContextShift[F]): F[Unit] =
    for {
      _ <- if (shouldSendSnapshotsToCloud) {
        sendSnapshotsToCloud[F](snapshots)
      } else Sync[F].unit
      _ <- snapshots.distinct.traverse { snapId =>
        withMetric(
          LiftIO[F].liftIO(dao.snapshotStorage.removeSnapshot(snapId).value.flatMap(IO.fromEither)).handleErrorWith {
            case e: NoSuchFileException =>
              Sync[F].delay(logger.warn(s"Snapshot to delete doesn't exist: ${e.getMessage}"))
          },
          "deleteSnapshot"
        )
      }
    } yield ()

  private def sendSnapshotsToCloud[F[_]: Concurrent](
    snapshotsHash: List[String]
  )(implicit dao: DAO, C: ContextShift[F]): F[Unit] =
    for {
      files <- C.evalOn(ConstellationExecutionContext.unbounded)(
        LiftIO[F].liftIO(dao.snapshotStorage.getSnapshotFiles(snapshotsHash))
      )
      blobsNames <- C.evalOn(ConstellationExecutionContext.unbounded)(
        LiftIO[F].liftIO(dao.cloudStorage.upload(files.toList))
      )
      _ <- Sync[F].delay(logger.debug(s"Snapshots send to cloud amount : ${blobsNames.size}"))
    } yield ()

  private def shouldSendSnapshotsToCloud: Boolean =
    ConfigUtil.isEnabledCloudStorage

  def isOverDiskCapacity(bytesLengthToAdd: Long)(implicit dao: DAO): IO[Boolean] = {
    val sizeDiskLimit = ConfigUtil.snapshotSizeDiskLimit
    if (sizeDiskLimit == 0) return false.pure[IO]

    val isOver = for {
      occupiedSpace <- dao.snapshotStorage.getOccupiedSpace
      usableSpace <- dao.snapshotStorage.getUsableSpace
      isOverSpace = occupiedSpace + bytesLengthToAdd > sizeDiskLimit || usableSpace < bytesLengthToAdd
    } yield isOverSpace

    isOver.flatTap { over =>
      IO.delay {
        if (over) {
          logger.warn(
            s"[${dao.id.short}] isOverDiskCapacity bytes to write ${bytesLengthToAdd} configured space: ${ConfigUtil.snapshotSizeDiskLimit}"
          )
        }
      }
    }
  }

  def loadSnapshot(snapshotHash: String)(implicit dao: DAO): Try[StoredSnapshot] =
    tryWithMetric(
      dao.snapshotStorage.readSnapshot(snapshotHash).value.flatMap(IO.fromEither).unsafeRunSync,
      "loadSnapshot"
    )

  def loadSnapshotBytes(snapshotHash: String)(implicit dao: DAO): Try[Array[Byte]] =
    tryWithMetric(
      dao.snapshotStorage.getSnapshotBytes(snapshotHash).value.flatMap(IO.fromEither).unsafeRunSync,
      "loadSnapshot"
    )

  def findLatestMessageWithSnapshotHash(
    depth: Int,
    lastMessage: Option[ChannelMessageMetadata],
    maxDepth: Int = 100
  )(implicit dao: DAO): Option[ChannelMessageMetadata] = {

    def findLatestMessageWithSnapshotHashInner(
      depth: Int,
      lastMessage: Option[ChannelMessageMetadata]
    ): Option[ChannelMessageMetadata] =
      if (depth > maxDepth) None
      else {
        lastMessage.flatMap { m =>
          if (m.snapshotHash.nonEmpty) Some(m)
          else {
            findLatestMessageWithSnapshotHashInner(
              depth + 1,
              dao.messageService.memPool
                .lookup(
                  m.channelMessage.signedMessageData.data.previousMessageHash
                )
                .unsafeRunSync()
            )
          }
        }
      }

    findLatestMessageWithSnapshotHashInner(depth, lastMessage)
  }

  val snapshotZero = Snapshot("", Seq(), Map.empty)
  val snapshotZeroHash: String = Snapshot("", Seq(), Map.empty).hash

}
