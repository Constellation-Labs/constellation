package org.constellation.storage

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.{Concurrent, ContextShift, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.DAO
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.{NodeState, NodeType}
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.schema.Id
import org.constellation.snapshot.SnapshotSelector
import org.constellation.storage.VerificationStatus.VerificationStatus
import org.constellation.util.HealthChecker

import scala.concurrent.duration._

class SnapshotBroadcastService[F[_]: Concurrent](
  healthChecker: HealthChecker[F],
  cluster: Cluster[F],
  snapshotSelector: SnapshotSelector[F],
  contextShift: ContextShift[F],
  dao: DAO
) extends StrictLogging {

  private val recentSnapshots: SingleRef[F, List[RecentSnapshot]] = SingleRef(List.empty[RecentSnapshot])

  val clusterCheckPending = new AtomicBoolean(false)

  def broadcastSnapshot(hash: String, height: Long, publicReputation: Map[Id, Double]): F[Unit] =
    for {
      ownRecent <- updateRecentSnapshots(hash, height, publicReputation)
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      responses <- peers.values.toList
        .traverse(
          _.client
            .postNonBlockingF[F, SnapshotVerification](
              "snapshot/verify",
              SnapshotCreated(hash, height, publicReputation),
              5 second
            )(
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
          healthChecker
            .startReDownload(d._1, peers.filter(p => d._1.peers.contains(p._1)))
            .flatMap(
              _ => recentSnapshots.set(d._2)
            )
      )
    } yield ()

  def verifyRecentSnapshots(): F[Unit] = {
    val verify = for {
      ownRecent <- getRecentSnapshots
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      responses <- snapshotSelector.collectSnapshot(peers)(contextShift)
      maybeDownload = snapshotSelector.selectSnapshotFromRecent(responses, ownRecent)
      _ <- maybeDownload.fold(Sync[F].unit)(
        d =>
          healthChecker
            .startReDownload(d._1, peers.filter(p => d._1.peers.contains(p._1)))
            .flatMap(
              _ =>
                recentSnapshots
                  .set(d._2)
            )
      )
    } yield ()

    if (clusterCheckPending.compareAndSet(false, true)) {
      cluster.getNodeState
        .map(NodeState.canVerifyRecentSnapshots)
        .ifM(verify, Sync[F].unit)
        .flatMap(_ => Sync[F].delay(clusterCheckPending.set(false)))
        .recover {
          case _ => clusterCheckPending.set(false)
        }
    } else {
      Sync[F].unit
    }
  }

  def getRecentSnapshots: F[List[RecentSnapshot]] = recentSnapshots.getUnsafe

  def runClusterCheck: F[Unit] =
    cluster.getNodeState
      .map(NodeState.canRunClusterCheck)
      .ifM(
        getRecentSnapshots
          .flatMap(healthChecker.checkClusterConsistency)
          .flatMap(
            maybeUpdate => maybeUpdate.fold(Sync[F].unit)(recentSnapshots.set)
          ),
        Sync[F].unit
      )

  def updateRecentSnapshots(hash: String, height: Long, publicReputation: Map[Id, Double]): F[List[RecentSnapshot]] =
    recentSnapshots.modify { snaps =>
      val updated =
        (RecentSnapshot(hash, height, publicReputation) :: snaps).slice(0, dao.processingConfig.recentSnapshotNumber)
      (updated, updated)
    }

  def shouldRunClusterCheck(responses: List[Option[SnapshotVerification]]): Boolean =
    responses.nonEmpty && ((responses.count(r => r.nonEmpty && r.get.status == VerificationStatus.SnapshotInvalid) * 100) / responses.size) >= dao.processingConfig.maxInvalidSnapshotRate
}

case class RecentSnapshot(hash: String, height: Long, publicReputation: Map[Id, Double])

case class SnapshotVerification(id: Id, status: VerificationStatus, recentSnapshot: List[RecentSnapshot])

object VerificationStatus extends Enumeration {
  type VerificationStatus = Value
  val SnapshotCorrect, SnapshotInvalid, SnapshotHeightAbove = Value
}
