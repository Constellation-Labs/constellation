package org.constellation.storage

import cats.effect.{Concurrent, ContextShift, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.DAO
import org.constellation.domain.schema.Id
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.primitives.Schema.NodeType
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.snapshot.SnapshotSelector
import org.constellation.storage.VerificationStatus.VerificationStatus
import org.constellation.util.HealthChecker

import scala.concurrent.duration._

class SnapshotBroadcastService[F[_]: Concurrent](
  healthChecker: HealthChecker[F],
  cluster: Cluster[F],
  snapshotSelector: SnapshotSelector,
  contextShift: ContextShift[F],
  dao: DAO
) extends StrictLogging {

  private val recentSnapshots: SingleRef[F, List[RecentSnapshot]] = SingleRef(List.empty[RecentSnapshot])

  def broadcastSnapshot(hash: String, height: Long): F[Unit] =
    for {
      ownRecent <- updateRecentSnapshots(hash, height)
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      responses <- peers.values.toList
        .traverse(
          _.client
            .postNonBlockingF[F, SnapshotVerification]("snapshot/verify", SnapshotCreated(hash, height), 5 second)(
              contextShift
            )
            .map(_.some)
            .handleErrorWith(
              t =>
                Sync[F]
                  .delay(logger.warn(s"error while verifying snapshot $hash msg: ${t.getMessage}"))
                  .flatMap(_ => Sync[F].pure[Option[SnapshotVerification]](None))
            )
        )
      maybeDownload = snapshotSelector.selectSnapshotFromBroadcastResponses(responses, ownRecent)
      _ <- maybeDownload.fold(Sync[F].unit)(
        d =>
          cluster.isNodeReady
            .ifM(healthChecker.startReDownload(d.diff, peers) >> recentSnapshots.set(d.recentStateToSet), Sync[F].unit)
      )
    } yield ()

  def verifyRecentSnapshots(): F[Unit] = {
    val verify = for {
      ownRecent <- getRecentSnapshots
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      responses <- collectSnapshot(peers)
      maybeDownload = snapshotSelector.selectSnapshotFromRecent(responses, ownRecent)
      _ <- maybeDownload.fold(Sync[F].unit)(
        d =>
          cluster.isNodeReady
            .ifM(healthChecker.startReDownload(d.diff, peers) >> recentSnapshots.set(d.recentStateToSet), Sync[F].unit)
      )
    } yield ()

    cluster.isNodeReady.ifM(verify, Sync[F].unit)
  }

  def getRecentSnapshots: F[List[RecentSnapshot]] = recentSnapshots.getUnsafe

  def runClusterCheck: F[Unit] =
    cluster.isNodeReady.ifM(
      getRecentSnapshots
        .flatMap(healthChecker.checkClusterConsistency)
        .flatMap(maybeUpdate => maybeUpdate.fold(Sync[F].unit)(recent => recentSnapshots.set(recent))),
      Sync[F].unit
    )

  def updateRecentSnapshots(hash: String, height: Long): F[List[RecentSnapshot]] =
    recentSnapshots.modify { snaps =>
      val updated = (RecentSnapshot(hash, height) :: snaps).slice(0, dao.processingConfig.recentSnapshotNumber)
      (updated, updated)
    }

  private def collectSnapshot(peers: Map[Id, PeerData]): F[List[(Id, List[RecentSnapshot])]] =
    peers.toList.traverse(
      p => (p._1, p._2.client.getNonBlockingF[F, List[RecentSnapshot]]("snapshot/recent")(contextShift)).sequence
    )

  def shouldRunClusterCheck(responses: List[Option[SnapshotVerification]]): Boolean =
    responses.nonEmpty && ((responses.count(r => r.nonEmpty && r.get.status == VerificationStatus.SnapshotInvalid) * 100) / responses.size) >= dao.processingConfig.maxInvalidSnapshotRate
}

case class RecentSnapshot(hash: String, height: Long)

case class SnapshotCreated(snapshot: String, height: Long)
case class SnapshotVerification(id: Id, status: VerificationStatus, recentSnapshot: List[RecentSnapshot])

object VerificationStatus extends Enumeration {
  type VerificationStatus = Value
  val SnapshotCorrect, SnapshotInvalid, SnapshotHeightAbove = Value
}
