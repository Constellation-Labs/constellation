package org.constellation.storage

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.DAO
import org.constellation.consensus.SnapshotInfo
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.{NodeState, NodeType}
import org.constellation.schema.Id
import org.constellation.snapshot.SnapshotSelector
import org.constellation.storage.VerificationStatus.VerificationStatus
import org.constellation.util.HealthChecker

import scala.concurrent.duration._
import scala.util.Try

class SnapshotBroadcastService[F[_]: Concurrent](
  healthChecker: HealthChecker[F],
  cluster: Cluster[F],
  snapshotSelector: SnapshotSelector[F],
  contextShift: ContextShift[F],
  dao: DAO
) extends StrictLogging {

  private val recentSnapshots: Ref[F, Map[Long, RecentSnapshot]] = Ref.unsafe(Map.empty)

  val clusterCheckPending = new AtomicBoolean(false)

  def applyAfterRedownload(snapshotInfo: SnapshotInfo): F[Unit] =
    Sync[F].unit

  def broadcastSnapshot(hash: String, height: Long, publicReputation: Map[Id, Double]): F[Unit] =
    for {
      ownRecent <- updateRecentSnapshots(hash, height, publicReputation)
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      _ <- Sync[F].delay{logger.warn(s"broadcastSnapshot for Id: ${dao.id} - ownRecent: ${ownRecent}")}
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
      maybeDownload = snapshotSelector.selectSnapshotFromBroadcastResponses(responses, ownRecent.values.toList)
      _ <- maybeDownload.fold(Sync[F].unit) {
        case (diff, _) =>
          logger.warn ("Redownload - broadcastSnapshot():")
          logger.warn ("Redownload - snapshots to delete:")
          diff.snapshotsToDelete.foreach(s => logger.warn(s" ${s.height} - ${s.hash}"))
          logger.warn ("Redownload - snapshots to download:")
          diff.snapshotsToDownload.foreach(s => logger.warn(s" ${s.height} - ${s.hash}"))
          healthChecker.startReDownload(diff, peers.filter(p => diff.peers.contains(p._1)))
      }
    } yield ()

  def verifyRecentSnapshots(): F[Unit] =
    Sync[F].unit
    val verify = for {
      ownRecent <- recentSnapshots.get
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      responses <- snapshotSelector.collectSnapshot(peers)(contextShift)
      maybeDownload = snapshotSelector.selectSnapshotFromRecent(responses, ownRecent.values.toList)
      _ <- Sync[F].delay(logger.warn(s"verifyRecentSnapshots - maybeDownload: ${maybeDownload}"))
      _ <- maybeDownload.fold(Sync[F].unit) {
        case (diff, _) =>
//          logger.warn(s"verifyRecentSnapshots - start reDownloadWith: ${diff} - peers: ${peers.filter(p => diff.peers.contains(p._1))}")

          logger.warn("Redownload - verifyRecentSnapshots():")
          logger.warn("Redownload - snapshots to delete:")
          diff.snapshotsToDelete.foreach(s => logger.warn(s" ${s.height} - ${s.hash}"))
          logger.warn("Redownload - snapshots to download:")
          diff.snapshotsToDownload.foreach(s => logger.warn(s" ${s.height} - ${s.hash}"))
          healthChecker.startReDownload(diff, peers.filter(p => diff.peers.contains(p._1)))
      }
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

  def getRecentSnapshots(snapHash: Option[String] = None): F[List[RecentSnapshot]] = {
    logger.debug(s"begin getRecentSnapshots for ${snapHash.toString}")
    val recentSnaps = Try { recentSnapshots.get.map(_.values.toSeq.sortBy(-_.height).toList) }
    if (recentSnaps.isFailure) {
      logger.debug(s"getRecentSnapshots failed with - ${recentSnaps} for ${snapHash.toString}")
      Sync[F].delay(Nil)
    }
    logger.debug(s"finish getRecentSnapshots with - ${recentSnaps} for ${snapHash.toString}")
    recentSnaps.get
  }

  def runClusterCheck: F[Unit] =
    cluster.getNodeState
      .map(NodeState.canRunClusterCheck)
      .ifM(
        getRecentSnapshots()
          .flatMap(healthChecker.checkClusterConsistency)
          .flatMap(
            maybeUpdate =>
              maybeUpdate.fold(Sync[F].unit)(a => recentSnapshots.modify(_ => (a.map(b => (b.height -> b)).toMap, ())))
          ),
        Sync[F].unit
      )

  def updateRecentSnapshots(
    hash: String,
    height: Long,
    publicReputation: Map[Id, Double]
  ): F[Map[Long, RecentSnapshot]] =
    recentSnapshots.modify { snaps =>
      val snap = (height -> RecentSnapshot(
        hash,
        height,
        publicReputation
      ))

      val updated = (snaps + snap).toList.sortBy(-_._2.height).slice(0, dao.processingConfig.recentSnapshotNumber).toMap
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
