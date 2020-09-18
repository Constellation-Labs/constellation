package org.constellation.util

import cats.effect.{Concurrent, ContextShift, LiftIO, Sync}
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.primitives.ConcurrentTipService
import org.constellation.schema.{Id, NodeState}
import org.constellation.{ConfigUtil, DAO}

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

  // TODO: unused, consider removing
  /*
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
   */

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
  apiClient: ClientInterpreter[F]
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

  private def collectNextSnapshotHeights(): F[Map[Id, Long]] =
    for {
      peers <- LiftIO[F]
        .liftIO(dao.cluster.getPeerInfo)
        .map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
      nextSnapshotHeights <- peers.values.toList
        .map(_.peerMetadata.toPeerClientMetadata)
        .traverse(
          a =>
            apiClient.snapshot
              .getNextSnapshotHeight()
              .run(a)
              .handleErrorWith(_ => (a.id, -1L).pure[F])
        )
        .map(_.filter { case (_, height) => height >= 0L })
    } yield nextSnapshotHeights.toMap

  def clearStaleTips(): F[Unit] =
    for {
      nextSnasphotHeights <- collectNextSnapshotHeights()
      heights = nextSnasphotHeights.values.toList
      nodesWithHeights = heights.filter(_ > 0)

      _ <- if (nodesWithHeights.size >= dao.processingConfig.numFacilitatorPeers) {

        /*
          nodes:            A     B     C
          delay: 10
          min tip height:   20    24    13
          next snapshot:    10    14    3

          --> node C should remove tip at height 13, as that tip will not be accepted by node A
         */

        val maxNextSnapshotHeight = nodesWithHeights.maximumOption

        if (maxNextSnapshotHeight.nonEmpty) {
          concurrentTipService.clearStaleTips(maxNextSnapshotHeight.get)
        } else logger.debug("[Clear stale tips] Not enough data to determine height")
      } else
        logger.debug(
          s"[Clear stale tips] Size=${nextSnasphotHeights.size} numFacilPeers=${dao.processingConfig.numFacilitatorPeers}"
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
