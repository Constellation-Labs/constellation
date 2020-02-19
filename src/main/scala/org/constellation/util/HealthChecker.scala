package org.constellation.util

import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.ConsensusManager
import org.constellation.p2p.Cluster
import org.constellation.primitives.ConcurrentTipService
import org.constellation.primitives.Schema.NodeType
import org.constellation.schema.Id
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

class MetricFailure(message: String) extends Exception(message)
case class HeightEmpty(nodeId: String) extends MetricFailure(s"Empty height found for node: $nodeId")
case class CheckPointValidationFailures(nodeId: String)
    extends MetricFailure(
      s"Checkpoint validation failures found for node: $nodeId"
    )
case class InconsistentSnapshotHash(nodeId: String, hashes: Set[String])
    extends MetricFailure(s"Node: $nodeId last snapshot hash differs: $hashes")

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
  calculationContext: ContextShift[F]
)(implicit C: ContextShift[F]) {

  implicit val shadedDao: DAO = dao

  val logger = Slf4jLogger.getLogger[F]

  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  val snapshotHeightDelayInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightDelayInterval")

  val snapshotHeightRedownloadDelayInterval: Int =
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval")

  def checkForStaleTips(): F[Unit] = {
    val check = for {
      _ <- logger.debug(s"[${dao.id.short}] Re-download checking cluster consistency")
      _ <- clearStaleTips()
    } yield ()

    check.recoverWith {
      case err =>
        logger
          .error(err)(s"[${dao.id.short}] Unexpected error during check for stale tips process: ${err.getMessage}")
          .flatMap(_ => Sync[F].pure[Unit](None))
    }
  }

  private def collectMinTipHeights(): F[Map[Id, Long]] =
    for {
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      minTipHeights <- peers.values.toList
        .map(_.client)
        .traverse(_.getNonBlockingF[F, (Id, Long)]("heights/min")(calculationContext))
    } yield minTipHeights.toMap

  def clearStaleTips(): F[Unit] =
    for {
      minTipHeights <- collectMinTipHeights()
      minHeights = minTipHeights.values.toList
      nodesWithHeights = minHeights.filter(_ > 0)

      _ = if (minHeights.size - nodesWithHeights.size < dao.processingConfig.numFacilitatorPeers && nodesWithHeights.size >= dao.processingConfig.numFacilitatorPeers) {
        val heightsOfMinimumFacilitators = nodesWithHeights
          .groupBy(a => a)
          .filter(_._2.size >= dao.processingConfig.numFacilitatorPeers)

        if (heightsOfMinimumFacilitators.nonEmpty) {
          concurrentTipService.clearStaleTips(minHeights.min)
        } else logger.debug("[Clear stale tips] Not enough data to determine height")
      } else
        logger.debug(
          s"[Clear stale tips] Size=${minTipHeights.size} numFacilPeers=${dao.processingConfig.numFacilitatorPeers}"
        )
    } yield ()

  /*
  def startReDownload(
    diff: SnapshotDiff,
    peers: Map[Id, PeerData]
  ): F[Unit] = {
    val reDownload = for {
      _ <- logger.info(s"[${dao.id.short}] Starting re-download process ${diff.snapshotsToDownload.size}")

      _ <- logger.debug(s"[${dao.id.short}] NodeState set to DownloadInProgress")

      _ <- LiftIO[F].liftIO(dao.terminateConsensuses())
      _ <- logger.debug(s"[${dao.id.short}] Consensuses terminated")

      _ <- downloader.reDownload(
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
 */

}
