package org.constellation.storage

import cats.effect.{Concurrent, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.DAO
import org.constellation.primitives.Schema.NodeType
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.storage.VerificationStatus.VerificationStatus
import org.constellation.util.HealthChecker

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
        peers.values.toList.traverse(
          _.client.postNonBlockingIO[SnapshotVerification]("snapshot/verify", SnapshotCreated(hash, height))
        )
      )
      _ <- if (hasInvalidSnapshot(responses))
        recentSnapshots.get.flatMap(recent => healthChecker.checkClusterConsistency(recent.map(_.hash)))
      else Sync[F].unit
    } yield ()

  def getRecentSnapshots: F[List[RecentSnapshot]] = recentSnapshots.get

  private[storage] def updateRecentSnapshots(hash: String, height: Long): F[Unit] =
    recentSnapshots.update(
      snaps => (RecentSnapshot(hash, height) :: snaps).slice(0, dao.processingConfig.recentSnapshotNumber)
    )

  def hasInvalidSnapshot(responses: List[SnapshotVerification]): Boolean =
    responses.nonEmpty && (responses.count(_.status == VerificationStatus.SnapshotInvalid) / responses.size) >= dao.processingConfig.maxInvalidSnapshotRate
}

case class RecentSnapshot(hash: String, height: Long)

case class SnapshotCreated(snapshot: String, height: Long)
case class SnapshotVerification(status: VerificationStatus)

object VerificationStatus extends Enumeration {
  type VerificationStatus = Value
  val SnapshotCorrect, SnapshotInvalid, SnapshotHeightAbove = Value
}
