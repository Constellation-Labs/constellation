package org.constellation.consensus

import java.nio.file.NoSuchFileException

import cats.data.EitherT
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.syntax.all._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache, CheckpointEdge}
import org.constellation.schema.signature.{HashSignature, Signable}
import org.constellation.schema.{ChannelMessageMetadata, Height, Id}
import org.constellation.serializer.KryoSerializer
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

import scala.collection.SortedMap
import scala.util.Try

case class CreateCheckpointEdgeResponse(
  checkpointEdge: CheckpointEdge,
  transactionsUsed: Set[String],
  updatedTransactionMemPoolThresholdMet: Set[String]
)

case class SignatureRequest(checkpointBlock: CheckpointBlock, facilitators: Set[Id])

object SignatureRequest {
  implicit val signatureRequestEncoder: Encoder[SignatureRequest] = deriveEncoder
  implicit val signatureRequestDecoder: Decoder[SignatureRequest] = deriveDecoder
}

case class SignatureResponse(signature: Option[HashSignature], reRegister: Boolean = false)

object SignatureResponse {
  implicit val signatureResponseEncoder: Encoder[SignatureResponse] = deriveEncoder
  implicit val signatureResponseDecoder: Decoder[SignatureResponse] = deriveDecoder
}

case class FinishedCheckpoint(checkpointCacheData: CheckpointCache, facilitators: Set[Id])

object FinishedCheckpoint {
  implicit val ord: Ordering[FinishedCheckpoint] =
    Ordering.by[FinishedCheckpoint, CheckpointCache](_.checkpointCacheData)

  implicit val finishedCheckpointEncoder: Encoder[FinishedCheckpoint] = deriveEncoder
  implicit val finishedCheckpointDecoder: Decoder[FinishedCheckpoint] = deriveDecoder
}

case class FinishedCheckpointResponse(isSuccess: Boolean = false)

case class TipData(checkpointBlock: CheckpointBlock, numUses: Int, height: Height)

object TipData {
  implicit val tipDataEncoder: Encoder[TipData] = deriveEncoder
  implicit val tipDataDecoder: Decoder[TipData] = deriveDecoder
}

case class Snapshot(lastSnapshot: String, checkpointBlocks: Seq[String], publicReputation: SortedMap[Id, Double])
    extends Signable {
  override def toEncode = checkpointBlocks :+ lastSnapshot
}

case class StoredSnapshot(snapshot: Snapshot, checkpointCache: Seq[CheckpointCache]) {

  def height: Long =
    checkpointCache.toList
      .maxBy(_.height.map(_.min).getOrElse(0L))
      .height
      .map(_.min)
      .getOrElse(0L)
}

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
                  .write(storedSnapshot.snapshot.hash, serialized)
                  .value
                  .flatMap(IO.fromEither)
              },
              "writeSnapshot"
            ).attemptT
          }
        }
    }

  def removeOldSnapshots[F[_]: Concurrent]()(implicit dao: DAO, C: ContextShift[F]): F[Unit] = {
    val removeF = for {
      createdHashes <- LiftIO[F].liftIO {
        dao.redownloadService.getCreatedSnapshots().map(_.values.toList.map(_.hash))
      }.attemptT
      acceptedHashes <- LiftIO[F].liftIO {
        dao.redownloadService.getAcceptedSnapshots().map(_.values.toList)
      }.attemptT
      storedHashes <- EitherT(LiftIO[F].liftIO(dao.snapshotStorage.list().value))

      diff = storedHashes.diff(createdHashes ++ acceptedHashes)

      _ <- removeSnapshots(diff).attemptT // TODO: To be confirmed
    } yield ()

    removeF.rethrowT
  }

  def removeSnapshots[F[_]: Concurrent](
    snapshots: List[String]
  )(implicit dao: DAO, C: ContextShift[F]): F[Unit] =
    for {
      _ <- snapshots.distinct.traverse { hash =>
        withMetric(
          LiftIO[F].liftIO(dao.snapshotStorage.delete(hash).rethrowT).handleErrorWith {
            case e: NoSuchFileException =>
              Sync[F].delay(logger.warn(s"Snapshot to delete doesn't exist: ${e.getMessage}"))
          },
          "deleteSnapshot"
        )
      }
      _ <- snapshots.distinct.traverse { hash =>
        withMetric(
          LiftIO[F]
            .liftIO(dao.snapshotInfoStorage.delete(hash).rethrowT)
            .handleErrorWith {
              case e: NoSuchFileException =>
                Sync[F].delay(logger.warn(s"Snapshot info to delete doesn't exist: ${e.getMessage}"))
            },
          "deleteSnapshotInfo"
        )
      }
    } yield ()

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
      dao.snapshotStorage.read(snapshotHash).rethrowT.unsafeRunSync,
      "loadSnapshot"
    )

  def loadSnapshotBytes(snapshotHash: String)(implicit dao: DAO): Try[Array[Byte]] =
    tryWithMetric(
      dao.snapshotStorage.readBytes(snapshotHash).rethrowT.unsafeRunSync,
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

  val snapshotZero = Snapshot("", Seq(), SortedMap.empty)
  val snapshotZeroHash: String = Snapshot("", Seq(), SortedMap.empty).hash

}
