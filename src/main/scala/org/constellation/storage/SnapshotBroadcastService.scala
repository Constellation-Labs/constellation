package org.constellation.storage

import cats.effect.{Concurrent, IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.DAO
import org.constellation.primitives.Schema.{NodeState, NodeType}
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.storage.VerificationStatus.VerificationStatus
import org.constellation.util.HealthChecker

import scala.concurrent.duration._

class SnapshotBroadcastService[F[_]: Concurrent](
  healthChecker: HealthChecker[F],
  dao: DAO
) extends StrictLogging {

  private val recentSnapshots: SingleRef[F, List[RecentSnapshot]] = SingleRef(List.empty[RecentSnapshot])

  def broadcastSnapshot(hash: String, height: Long): F[Unit] =
    for {
      _ <- updateRecentSnapshots(hash, height)
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      responses <- LiftIO[F].liftIO(
        peers.values.toList
          .traverse(
            _.client
              .postNonBlockingIO[SnapshotVerification]("snapshot/verify", SnapshotCreated(hash, height), 5 second)
              .handleErrorWith(
                t =>
                  IO(logger.warn(s"error while verifying snapshot $hash msg: ${t.getMessage}"))
                    .flatMap(_ => IO.pure(SnapshotVerification(VerificationStatus.SnapshotInvalid)))
              )
          )
      )
      isInvalid = hasInvalidSnapshot(responses)
      _ <- if (isInvalid)
        runClusterCheck
      else Sync[F].unit
    } yield ()

  def getRecentSnapshots: F[List[RecentSnapshot]] = recentSnapshots.getUnsafe

  def runClusterCheck: F[Unit] =
    if (dao.nodeState == NodeState.Ready) {
      getRecentSnapshots
        .flatMap(healthChecker.checkClusterConsistency)
        .flatMap(maybeUpdate => maybeUpdate.fold(Sync[F].unit)(recent => recentSnapshots.set(recent)))
    } else {
      Sync[F].unit
    }

  def updateRecentSnapshots(hash: String, height: Long): F[Unit] =
    recentSnapshots.update(
      snaps => (RecentSnapshot(hash, height) :: snaps).slice(0, dao.processingConfig.recentSnapshotNumber)
    )

  def hasInvalidSnapshot(responses: List[SnapshotVerification]): Boolean =
    responses.nonEmpty && ((responses.count(_.status == VerificationStatus.SnapshotInvalid) * 100) / responses.size) >= dao.processingConfig.maxInvalidSnapshotRate
}

case class RecentSnapshot(hash: String, height: Long)

case class SnapshotCreated(snapshot: String, height: Long)
case class SnapshotVerification(status: VerificationStatus)

object VerificationStatus extends Enumeration {
  type VerificationStatus = Value
  val SnapshotCorrect, SnapshotInvalid, SnapshotHeightAbove = Value
}
