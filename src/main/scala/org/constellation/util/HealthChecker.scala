package org.constellation.util

import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.{ConsensusManager, Snapshot}
import org.constellation.p2p.{Cluster, DownloadProcess, PeerData}
import org.constellation.primitives.ConcurrentTipService
import org.constellation.primitives.Schema.{NodeState, NodeType}
import org.constellation.schema.Id
import org.constellation.storage._
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

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

object HealthChecker {
  val snapshotHeightDelayInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightDelayInterval")

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

class HealthChecker[F[_]: Concurrent](
  dao: DAO,
  concurrentTipService: ConcurrentTipService[F],
  consensusManager: ConsensusManager[F],
  calculationContext: ContextShift[F],
  downloader: DownloadProcess[F],
  cluster: Cluster[F],
  majorityStateChooser: MajorityStateChooser[F]
)(implicit C: ContextShift[F]) {

  implicit val shadedDao: DAO = dao

  val logger = Slf4jLogger.getLogger[F]

  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  def checkClusterConsistency(ownSnapshots: List[RecentSnapshot]): F[Unit] = {
    val check = for {
      _ <- logger.debug(s"[${dao.id.short}] [HealthChecker] checking cluster consistency")
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      peersSnapshots <- collectSnapshot(peers)
      _ <- clearStaleTips(peersSnapshots)
    } yield ()

    check.recoverWith {
      case err =>
        logger
          .error(err)(s"[${dao.id.short}] Unexpected error during check cluster consistency: ${err.getMessage}")
          .flatMap(_ => Sync[F].pure[Unit](()))
    }
  }

  def clearStaleTips(clusterSnapshots: List[(Id, List[RecentSnapshot])]): F[Unit] = {
    val nodesWithHeights = clusterSnapshots.filter(_._2.nonEmpty)
    if (clusterSnapshots.size - nodesWithHeights.size < dao.processingConfig.numFacilitatorPeers && nodesWithHeights.size >= dao.processingConfig.numFacilitatorPeers) {
      val maxHeightsOfMinimumFacilitators = nodesWithHeights
        .map(x => x._2.map(_.height).max)
        .groupBy(x => x)
        .filter(t => t._2.size >= dao.processingConfig.numFacilitatorPeers)

      if (maxHeightsOfMinimumFacilitators.nonEmpty)
        concurrentTipService.clearStaleTips(
          maxHeightsOfMinimumFacilitators.keySet.min + snapshotHeightInterval
        )
      else logger.debug("[Clear staletips] staletips Not enough data to determine height")
    } else
      logger.debug(
        s"[Clear staletips] ClusterSnapshots size=${clusterSnapshots.size} numFacilPeers=${dao.processingConfig.numFacilitatorPeers}"
      )
  }

  def startReDownload(
    diff: SnapshotDiff,
    peers: Map[Id, PeerData]
  ): F[Unit] = {
    val reDownload = for {
      _ <- logger.info(s"[${dao.id.address}] Starting re-download process for snapshots ${diff.snapshotsToDownload} from peers ${diff.peers}")

      _ <- logger.debug(s"[${dao.id.short}] NodeState set to DownloadInProgress")

      _ <- LiftIO[F].liftIO(dao.terminateConsensuses())
      _ <- logger.debug(s"[${dao.id.short}] Consensuses terminated")
      majoritySnapshot = diff.snapshotsToDownload.maxBy(_.height)
      _ <- downloader.reDownload(
        majoritySnapshot.height,
        diff.snapshotsToDownload.map(_.hash).filterNot(_ == Snapshot.snapshotZeroHash),
        peers.filterKeys(diff.peers.contains)
      )

      _ <- Snapshot.removeSnapshots(
        diff.snapshotsToDelete.map(_.hash).filterNot(_ == Snapshot.snapshotZeroHash)
      )

      _ <- logger.info(s"[${dao.id.short}] Re-download process finished")
      _ <- dao.metrics.incrementMetricAsync(Metrics.reDownloadFinished)
    } yield ()

    val wrappedDownload =
      cluster.compareAndSet(NodeState.validForRedownload, NodeState.DownloadInProgress).flatMap { stateSetResult =>
        if (stateSetResult.isNewSet) {
          val recover = reDownload.handleErrorWith { err =>
            for {
              _ <- logger.error(err)(s"[${dao.id.short}] re-download process error: ${err.getMessage}")
              recoverSet <- cluster.compareAndSet(NodeState.validDuringDownload, stateSetResult.oldState)
              _ <- logger
                .info(s"[${dao.id.short}] trying set state back to: ${stateSetResult.oldState} result: ${recoverSet}")
              _ <- dao.metrics.incrementMetricAsync(Metrics.reDownloadError)
              _ <- Sync[F].raiseError[Unit](err)
            } yield ()
          }

          recover.flatMap(
            _ =>
              cluster
                .compareAndSet(NodeState.validDuringDownload, stateSetResult.oldState)
                .void
          )
        } else {
          logger.warn(s"Download process can't start due to invalid node state: ${stateSetResult.oldState}")
        }
      }

    wrappedDownload
  }

  private def collectSnapshot(peers: Map[Id, PeerData]): F[List[(Id, List[RecentSnapshot])]] =
    peers.toList.traverse(
      p => (p._1, p._2.client.getNonBlockingF[F, List[RecentSnapshot]]("snapshot/recent")(calculationContext)).sequence
    )

}
