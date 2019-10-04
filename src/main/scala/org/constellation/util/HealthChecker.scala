package org.constellation.util

import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.chrisdavenport.log4cats.Logger
import org.constellation.consensus.{ConsensusManager, Snapshot}
import org.constellation.p2p.{DownloadProcess, PeerData}
import org.constellation.primitives.ConcurrentTipService
import org.constellation.primitives.Schema.{NodeState, NodeType}
import org.constellation.domain.schema.Id
import org.constellation.storage._
import org.constellation.util.HealthChecker.maxOrZero
import org.constellation.{ConstellationExecutionContext, DAO}

class MetricFailure(message: String) extends Exception(message)
case class HeightEmpty(nodeId: String) extends MetricFailure(s"Empty height found for node: $nodeId")
case class CheckPointValidationFailures(nodeId: String)
    extends MetricFailure(
      s"Checkpoint validation failures found for node: $nodeId"
    )
case class InconsistentSnapshotHash(nodeId: String, hashes: Set[String])
    extends MetricFailure(s"Node: $nodeId last snapshot hash differs: $hashes")
case class SnapshotDiff(
  snapshotsToDelete: List[RecentSnapshot],
  snapshotsToDownload: List[RecentSnapshot],
  peers: List[Id]
)

object HealthChecker extends StrictLogging {

  private def maxOrZero(list: List[RecentSnapshot]): Long =
    list match {
      case Nil      => 0
      case nonEmpty => nonEmpty.map(_.height).max
    }

  def checkAllMetrics(apis: Seq[APIClient]): Either[MetricFailure, Unit] = {
    var hashes: Set[String] = Set.empty
    val it = apis.iterator
    var lastCheck: Either[MetricFailure, Unit] = Right(())
    while (it.hasNext && lastCheck.isRight) {
      val a = it.next()
      val metrics = IO
        .fromFuture(IO { a.metricsAsync })(IO.contextShift(ConstellationExecutionContext.bounded))
        .unsafeRunSync() // TODO: wkoszycki revisit
      lastCheck = checkLocalMetrics(metrics, a.baseURI).orElse {
        hashes ++= Set(metrics.getOrElse(Metrics.lastSnapshotHash, "no_snap"))
        Either.cond(hashes.size == 1, (), InconsistentSnapshotHash(a.baseURI, hashes))
      }
    }
    lastCheck
  }

  def checkLocalMetrics(metrics: Map[String, String], nodeId: String): Either[MetricFailure, Unit] =
    hasEmptyHeight(metrics, nodeId)
      .orElse(hasCheckpointValidationFailures(metrics, nodeId))

  def hasEmptyHeight(metrics: Map[String, String], nodeId: String): Either[MetricFailure, Unit] =
    Either.cond(!metrics.contains(Metrics.heightEmpty), (), HeightEmpty(nodeId))

  def hasCheckpointValidationFailures(metrics: Map[String, String], nodeId: String): Either[MetricFailure, Unit] =
    Either.cond(!metrics.contains(Metrics.checkpointValidationFailure), (), CheckPointValidationFailures(nodeId))
}

case class RecentSync(hash: String, height: Long)

class HealthChecker[F[_]: Concurrent: Logger](
  dao: DAO,
  concurrentTipService: ConcurrentTipService[F],
  consensusManager: ConsensusManager[F],
  calculationContext: ContextShift[F],
  downloader: DownloadProcess[F],
  majorityStateChooser: MajorityStateChooser[F]
)(implicit C: ContextShift[F])
    extends StrictLogging {

  implicit val shadedDao: DAO = dao

  def checkClusterConsistency(ownSnapshots: List[RecentSnapshot]): F[Option[List[RecentSnapshot]]] = {
    val check = for {
      _ <- Logger[F].info(s"[${dao.id.short}] Re-download checking cluster consistency")
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      peersSnapshots <- collectSnapshot(peers)
      _ <- clearStaleTips(peersSnapshots)

//      major <- majorityStateChooser
//        .chooseMajorityState(peersSnapshots :+ (dao.id, ownSnapshots), maxOrZero(ownSnapshots))
//        .getOrElse((Seq[RecentSnapshot](), Set[Id]()))
//
//      diff <- compareSnapshotState(major, ownSnapshots)

      result <- Sync[F].pure[Option[List[RecentSnapshot]]](None)
//      result <- if (shouldReDownload(ownSnapshots, diff)) {
//        Logger[F].info(
//          s"[${dao.id.short}] Re-download process with : " +
//            s"Snapshot to download : ${diff.snapshotsToDownload} " +
//            s"Snapshot to delete : ${diff.snapshotsToDelete} " +
//            s"From peers : ${diff.peers} " +
//            s"Own snapshots : $ownSnapshots " +
//            s"Major state : $major"
//        ) >>
//          startReDownload(diff, peers.filterKeys(diff.peers.contains))
//            .flatMap(_ => Sync[F].delay[Option[List[RecentSnapshot]]](Some(major._1.toList)))
//      } else {
//        Sync[F].pure[Option[List[RecentSnapshot]]](None)
//      }
    } yield result

    check.recoverWith {
      case err =>
        Logger[F]
          .error(err)(s"[${dao.id.short}] Unexpected error during re-download process: ${err.getMessage}")
          .flatMap(_ => Sync[F].pure[Option[List[RecentSnapshot]]](None))
    }
  }

  private[util] def compareSnapshotState(major: (Seq[RecentSnapshot], Set[Id]), ownSnapshots: List[RecentSnapshot]) =
    SnapshotDiff(
      ownSnapshots.diff(major._1).sortBy(_.height).reverse,
      major._1.diff(ownSnapshots).toList.sortBy(_.height).reverse,
      major._2.toList
    ).pure[F]

  def clearStaleTips(clusterSnapshots: List[(Id, List[RecentSnapshot])]): F[Unit] = {
    val nodesWithHeights = clusterSnapshots.filter(_._2.nonEmpty)
    logger.info(s"[Clear staletips] Nodes with heights: ${nodesWithHeights.size}")
    if (clusterSnapshots.size - nodesWithHeights.size < dao.processingConfig.numFacilitatorPeers && nodesWithHeights.size >= dao.processingConfig.numFacilitatorPeers) {
      val maxHeightsOfMinimumFacilitators = nodesWithHeights
        .map(x => x._2.map(_.height).max)
        .groupBy(x => x)
        .filter(t => t._2.size >= dao.processingConfig.numFacilitatorPeers)
      logger.info(s"[Clear staletips] Max Heights Of Minimum Facilitators>: ${maxHeightsOfMinimumFacilitators.size}")

      if (maxHeightsOfMinimumFacilitators.nonEmpty)
        concurrentTipService.clearStaleTips(
          maxHeightsOfMinimumFacilitators.keySet.min + dao.processingConfig.snapshotHeightInterval
        )
      else Logger[F].debug("[Clear staletips] staletips Not enough data to determine height")
    } else
      Logger[F].debug(
        s"[Clear staletips] ClusterSnapshots size=${clusterSnapshots.size} numFacilPeers=${dao.processingConfig.numFacilitatorPeers}"
      ) >> Sync[F].unit
  }

  def shouldReDownload(ownSnapshots: List[RecentSnapshot], diff: SnapshotDiff): Boolean =
    diff match {
      case SnapshotDiff(_, _, Nil) => false
      case SnapshotDiff(_, Nil, _) => false
      case SnapshotDiff(snapshotsToDelete, snapshotsToDownload, _) =>
        val below = isBelowInterval(ownSnapshots, snapshotsToDownload)
        val misaligned =
          isMisaligned(ownSnapshots, (snapshotsToDelete ++ snapshotsToDownload).map(r => (r.height, r.hash)).toMap)
        logger.debug(s"Should re-download isBelow : $below : isMisaligned : $misaligned")
        below || misaligned
    }

  private def isMisaligned(ownSnapshots: List[RecentSnapshot], recent: Map[Long, String]) =
    ownSnapshots.exists(r => recent.get(r.height).exists(_ != r.hash))

  private def isBelowInterval(ownSnapshots: List[RecentSnapshot], snapshotsToDownload: List[RecentSnapshot]) =
    (maxOrZero(ownSnapshots) + dao.processingConfig.snapshotHeightRedownloadDelayInterval) < maxOrZero(
      snapshotsToDownload
    )

  def startReDownload(
    diff: SnapshotDiff,
    peers: Map[Id, PeerData]
  ): F[Unit] = {
    val reDownload = for {
      _ <- Logger[F].info(s"[${dao.id.short}] Starting re-download process")

      _ <- downloader.setNodeState(NodeState.DownloadInProgress)
      _ <- Logger[F].debug(s"[${dao.id.short}] NodeState set to DownloadInProgress")

      _ <- LiftIO[F].liftIO(dao.terminateConsensuses())
      _ <- Logger[F].debug(s"[${dao.id.short}] Consensuses terminated")

      _ <- downloader.reDownload(
        diff.snapshotsToDownload.map(_.hash).filterNot(_ == Snapshot.snapshotZeroHash),
        peers.filterKeys(diff.peers.contains)
      )

      _ <- Snapshot.removeSnapshots(
        diff.snapshotsToDelete.map(_.hash).filterNot(_ == Snapshot.snapshotZeroHash),
        dao.snapshotPath.pathAsString
      )

      _ <- Logger[F].info(s"[${dao.id.short}] Re-download process finished")
      _ <- dao.metrics.incrementMetricAsync(Metrics.reDownloadFinished)
    } yield ()

    reDownload.recoverWith {
      case err =>
        for {
          _ <- downloader.setNodeState(NodeState.Ready)
          _ <- Logger[F].error(err)(s"[${dao.id.short}] re-download process error: ${err.getMessage}")
          _ <- dao.metrics.incrementMetricAsync(Metrics.reDownloadError)
          _ <- Sync[F].raiseError[Unit](err)
        } yield ()
    }
  }

  private def collectSnapshot(peers: Map[Id, PeerData]): F[List[(Id, List[RecentSnapshot])]] =
    peers.toList.traverse(
      p => (p._1, p._2.client.getNonBlockingF[F, List[RecentSnapshot]]("snapshot/recent")(calculationContext)).sequence
    )

}
