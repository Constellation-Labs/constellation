package org.constellation.util

import cats.effect.{Blocker, Concurrent, ContextShift, LiftIO, Sync}
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.CheckpointService
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.Cluster
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
  checkpointService: CheckpointService[F],
  apiClient: ClientInterpreter[F],
  unboundedBlocker: Blocker,
  id: Id,
  clusterStorage: ClusterStorageAlgebra[F],
  numFacilitatorPeers: Int
)(implicit C: ContextShift[F]) {

  val logger = Slf4jLogger.getLogger[F]

  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  val snapshotHeightDelayInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightDelayInterval")

  val snapshotHeightRedownloadDelayInterval: Int =
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval")

  def checkForStaleTips(): F[Unit] = {
    val check = for {
      _ <- logger.debug(s"[${id.short}] Re-download checking cluster consistency")
      _ <- clearStaleTips()
    } yield ()

    check.recoverWith {
      case err =>
        logger
          .error(err)(s"[${id.short}] Unexpected error during check for stale tips process: ${err.getMessage}")
          .flatMap(_ => Sync[F].pure[Unit](None))
    }
  }

  private def collectNextSnapshotHeights(): F[Map[Id, Long]] =
    for {
      peers <- clusterStorage.getPeers
        .map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
      nextSnapshotHeights <- peers.values.toList
        .map(_.peerMetadata.toPeerClientMetadata)
        .traverse(
          a =>
            PeerResponse
              .run(
                apiClient.snapshot
                  .getNextSnapshotHeight(),
                unboundedBlocker
              )(a)
              .handleErrorWith(_ => (a.id, -1L).pure[F])
        )
        .map(_.filter { case (_, height) => height >= 0L })
    } yield nextSnapshotHeights.toMap

  def clearStaleTips(): F[Unit] =
    for {
      nextSnasphotHeights <- collectNextSnapshotHeights()
      heights = nextSnasphotHeights.values.toList
      nodesWithHeights = heights.filter(_ > 0)

      _ <- if (nodesWithHeights.size >= numFacilitatorPeers) {

        /*
          nodes:            A     B     C
          delay: 10
          min tip height:   20    24    13
          next snapshot:    10    14    3

          --> node C should remove tip at height 13, as that tip will not be accepted by node A
         */

        val maxNextSnapshotHeight = nodesWithHeights.maximumOption

        if (maxNextSnapshotHeight.nonEmpty) {
          checkpointService.clearStaleTips(maxNextSnapshotHeight.get)
        } else logger.debug("[Clear stale tips] Not enough data to determine height")
      } else
        logger.debug(
          s"[Clear stale tips] Size=${nextSnasphotHeights.size} numFacilPeers=${numFacilitatorPeers}"
        )
    } yield ()

}
