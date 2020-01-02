package org.constellation.consensus

import java.io.IOException
import java.nio.file.{NoSuchFileException, Path}

import better.files.File
import cats.data.{EitherT, NonEmptyList}
import cats.data.Validated.{Invalid, Valid}
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
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
import scala.util.{Failure, Try}

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


//  def toSnapshotInfoSer(info: SnapshotInfo) = {
//    SnapshotInfoSer(
//      KryoSerializer.serializeAnyRef(info.snapshot),
//      KryoSerializer.serializeAnyRef(info.acceptedCBSinceSnapshot),
//      KryoSerializer.serializeAnyRef(info.acceptedCBSinceSnapshotCache),
//      info.lastSnapshotHeight,
//      KryoSerializer.serializeAnyRef(info.snapshotHashes),
//      KryoSerializer.serializeAnyRef(info.addressCacheData),
//      KryoSerializer.serializeAnyRef(info.tips),
//      KryoSerializer.serializeAnyRef(info.snapshotCache),
//      KryoSerializer.serializeAnyRef(info.lastAcceptedTransactionRef)
//    )
//  }

  def toSnapshotInfoSer(info: SnapshotInfo, chunkSize: Int = 100) = {//todo make chunk size config
//    val test: Iterator[Map[String, AddressCacheData]] = info.addressCacheData.grouped(100)
    SnapshotInfoSer(
      chunkSerialize(Seq(info.snapshot), "info.snapshot"),
      info.acceptedCBSinceSnapshot.grouped(chunkSize).map(t => chunkSerialize(t.toSeq, "acceptedCBSinceSnapshot")).toArray,
      info.acceptedCBSinceSnapshotCache.grouped(chunkSize).map(t => chunkSerialize(t.toSeq, "acceptedCBSinceSnapshotCache")).toArray,
      info.lastSnapshotHeight,
      info.snapshotHashes.grouped(chunkSize).map(t => chunkSerialize(t.toSeq, "snapshotHashes")).toArray,
      info.addressCacheData.toSeq.grouped(chunkSize).map(partitionMap => chunkSerialize(partitionMap.toSeq, "addressCacheData")).toArray,
      info.tips.grouped(chunkSize).map(partitionMap => chunkSerialize(partitionMap.toSeq, "acceptedCBSinceSnapshot")).toArray,
      info.snapshotCache.grouped(chunkSize).map(t => chunkSerialize(t.toSeq, "snapshotCache")).toArray,
      info.lastAcceptedTransactionRef.grouped(chunkSize).map(partitionMap => chunkSerialize(partitionMap.toSeq, "lastAcceptedTransactionRef")).toArray
    )
  }

  def chunkSerialize[T](chunk: Seq[T], tag: String) = {
    logger.warn(s"chunkSerialize ${tag}")
    KryoSerializer.serializeAnyRef(chunk)
  }

  def chunkDeSerialize[T](chunk: Array[Byte], tag: String) = {
    logger.warn(s"chunkDeSerialize ${tag}")
    KryoSerializer.deserializeCast[T](chunk)
  }

  def toSnapshotInfo(info: SnapshotInfoSer) = {
    SnapshotInfo(
      chunkDeSerialize[Seq[Snapshot]](info.snapshot, "info.snapshot").head,
      info.acceptedCBSinceSnapshot.toSeq.flatMap(chunkDeSerialize[Seq[String]](_, "acceptedCBSinceSnapshot")),
      info.acceptedCBSinceSnapshotCache.toSeq.flatMap(chunkDeSerialize[Seq[CheckpointCache]](_, "acceptedCBSinceSnapshot")),
      info.lastSnapshotHeight,
      info.snapshotHashes.toSeq.flatMap(chunkDeSerialize[Seq[String]](_, "snapshotHashes")),
      info.addressCacheData.toSeq.flatMap(chunkDeSerialize[Seq[(String, AddressCacheData)]](_, "addressCacheData")).toMap,
      info.tips.toSeq.flatMap(chunkDeSerialize[Seq[(String, TipData)]](_, "tips")).toMap,
      info.snapshotCache.toSeq.flatMap(chunkDeSerialize[Seq[CheckpointCache]](_, "snapshotCache")),
      info.lastAcceptedTransactionRef.toSeq.flatMap(chunkDeSerialize[Seq[(String, LastTransactionRef)]](_, "lastAcceptedTransactionRef")).toMap
    )
  }
}

case class TipData(checkpointBlock: CheckpointBlock, numUses: Int, height: Height)

case class SnapshotInfo(
  snapshot: Snapshot,
  acceptedCBSinceSnapshot: Seq[String] = Seq(),//todo remove
  acceptedCBSinceSnapshotCache: Seq[CheckpointCache] = Seq(),
  lastSnapshotHeight: Int = 0,
  snapshotHashes: Seq[String] = Seq(),
  addressCacheData: Map[String, AddressCacheData] = Map(),
  tips: Map[String, TipData] = Map(),
  snapshotCache: Seq[CheckpointCache] = Seq(),
  lastAcceptedTransactionRef: Map[String, LastTransactionRef] = Map()
)

case class SnapshotInfoSer(  snapshot: Array[Byte],
                             acceptedCBSinceSnapshot: Array[Array[Byte]],
                             acceptedCBSinceSnapshotCache: Array[Array[Byte]],
                             lastSnapshotHeight: Int,
                             snapshotHashes: Array[Array[Byte]],
                             addressCacheData: Array[Array[Byte]],
                             tips: Array[Array[Byte]],
                             snapshotCache: Array[Array[Byte]],
                             lastAcceptedTransactionRef: Array[Array[Byte]])

case object GetMemPool

case class Snapshot(lastSnapshot: String, checkpointBlocks: Seq[String]) extends Signable

case class StoredSnapshot(snapshot: Snapshot, checkpointCache: Seq[CheckpointCache])

case class DownloadComplete(latestSnapshot: Snapshot)

import java.nio.file.{Files, Paths}

object Snapshot extends StrictLogging {

  val maxSnapshotInDir = ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval")
  var snapshotDir: Long = 0//todo ref?
  var curSnapshotsInDir = 0//todo ref?

  def getDirFromSnapshotCounter = {
    println("curSnapshotsInDir"+ curSnapshotsInDir )
    println("snapshotDirs"+ snapshotDir )
    if (curSnapshotsInDir >= maxSnapshotInDir) {
      snapshotDir += 1

      snapshotDir
    }
    else {
      curSnapshotsInDir += 1
      snapshotDir
    }
  }

  def writeSnapshot[F[_]: Concurrent](
    storedSnapshot: StoredSnapshot
  )(implicit dao: DAO, C: ContextShift[F]): EitherT[F, Throwable, Path] =
    for {
      serialized <- EitherT(Sync[F].delay(KryoSerializer.serializeAnyRef(storedSnapshot)).attempt)
      write <- EitherT(
        C.evalOn(ConstellationExecutionContext.unbounded)(writeSnapshot(storedSnapshot, serialized//, directory = getDirFromSnapshotCounter
        ).value)
      )
    } yield write

  private def writeSnapshot[F[_]: Concurrent](
    storedSnapshot: StoredSnapshot,
    serialized: Array[Byte],
//    directory: Long = 0L,
    trialNumber: Int = 0
  )(
    implicit dao: DAO,
    C: ContextShift[F]
  ): EitherT[F, Throwable, Path] =
    trialNumber match {
      case x if x >= 3 => EitherT.leftT[F, Path](new IOException(s"Unable to write snapshot"))
      case _ if isOverDiskCapacity(serialized.length) =>
        EitherT(removeOldSnapshots().attempt) >> writeSnapshot(storedSnapshot, serialized,
//          directory,
          trialNumber + 1)
      case _ =>
        EitherT(
          withMetric(
            Sync[F].delay(
              Files.write(Paths.get(dao.snapshotPath.pathAsString,// + s"/${directory}",
                storedSnapshot.snapshot.hash), serialized)
            ),
            "writeSnapshot"
          ).attempt
        )
    }

  def removeOldSnapshots[F[_]: Concurrent]()(implicit dao: DAO, C: ContextShift[F]): F[Unit] =
    for {
      hashes <- LiftIO[F].liftIO(dao.snapshotBroadcastService.getRecentSnapshots.map(_.map(_.hash)))
      diff = snapshotHashes().diff(hashes)
      _ <- removeSnapshots(diff, dao.snapshotPath.pathAsString)
    } yield ()

  def removeSnapshots[F[_]: Concurrent](
    snapshots: List[String],
    snapshotPath: String
  )(implicit dao: DAO, C: ContextShift[F]): F[Unit] =
    for {
      _ <- if (shouldSendSnapshotsToCloud(snapshotPath)) {
        sendSnapshotsToCloud[F](snapshots)
      } else Sync[F].unit
      _ <- snapshots.distinct.traverse { snapId =>
        withMetric(
          Sync[F].delay {
            logger.debug(
              s"[${dao.id.short}] removing snapshot at path ${Paths.get(snapshotPath, snapId).toAbsolutePath.toString}"
            )
            Files.delete(Paths.get(snapshotPath, snapId))
          }.handleErrorWith {
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
        getFiles(snapshotsHash, dao.snapshotPath.pathAsString)
      )
      blobsNames <- C.evalOn(ConstellationExecutionContext.unbounded)(LiftIO[F].liftIO(dao.cloudStorage.upload(files)))
      _ <- Sync[F].delay(logger.debug(s"Snapshots send to cloud amount : ${blobsNames.size}"))
    } yield ()

  private def getFiles[F[_]: Concurrent](snapshotsHash: List[String], snapshotPath: String): F[List[File]] =
    snapshotsHash.traverse(hash => Sync[F].delay(File(snapshotPath, hash)))

  private def shouldSendSnapshotsToCloud(snapshotsPath: String): Boolean =
    ConfigUtil.getOrElse("constellation.storage.enabled", default = false)

  def isOverDiskCapacity(bytesLengthToAdd: Long)(implicit dao: DAO): Boolean = {
    val sizeDiskLimit = ConfigUtil.snapshotSizeDiskLimit
    if (sizeDiskLimit == 0) return false

    val storageDir = new java.io.File(dao.snapshotPath.pathAsString)
    val usableSpace = storageDir.getUsableSpace
    val occupiedSpace = dao.snapshotPath.size
    val isOver = occupiedSpace + bytesLengthToAdd > sizeDiskLimit || usableSpace < bytesLengthToAdd
    if (isOver) {
      logger.warn(
        s"[${dao.id.short}] isOverDiskCapacity bytes to write ${bytesLengthToAdd} configured space: ${ConfigUtil.snapshotSizeDiskLimit} occupied space: $occupiedSpace usable space: $usableSpace"
      )
    }
    isOver
  }

  def loadSnapshot(snapshotHash: String)(implicit dao: DAO): Try[StoredSnapshot] =
    tryWithMetric(
      {
        KryoSerializer.deserializeCast[StoredSnapshot] {
          val byteArray = Files.readAllBytes(Paths.get(dao.snapshotPath.pathAsString, snapshotHash))
          //   val f = File(dao.snapshotPath, snapshotHash)
          byteArray
        }
      },
      "loadSnapshot"
    )

  def loadSnapshotBytes(snapshotHash: String)(implicit dao: DAO): Try[Array[Byte]] =
    tryWithMetric(
      {
        val path = Paths.get(dao.snapshotPath.pathAsString, snapshotHash)
        if (Files.exists(path)) {
          val byteArray = Files.readAllBytes(path)
          //   val f = File(dao.snapshotPath, snapshotHash)
          byteArray
        } else throw new RuntimeException(s"${dao.id.short} No snapshot found at $path")
      },
      "loadSnapshot"
    )

  def snapshotHashes()(implicit dao: DAO): List[String] =
    dao.snapshotPath.list.map { _.name }.toList//todo this is duplicated

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

  val snapshotZero = Snapshot("", Seq())
  val snapshotZeroHash: String = Snapshot("", Seq()).hash

}
