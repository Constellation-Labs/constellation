package org.constellation.util
import cats.effect.{Concurrent, IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.consensus.Snapshot
import org.constellation.p2p.DownloadProcess
import org.constellation.primitives.PeerData
import org.constellation.primitives.Schema.{Id, NodeState, NodeType}
import org.constellation.storage._
import org.constellation.util.HealthChecker.compareSnapshotState
import org.constellation.{ConstellationContextShift, DAO}

class MetricFailure(message: String) extends Exception(message)
case class HeightEmpty(nodeId: String) extends MetricFailure(s"Empty height found for node: $nodeId")
case class CheckPointValidationFailures(nodeId: String)
    extends MetricFailure(
      s"Checkpoint validation failures found for node: $nodeId"
    )
case class InconsistentSnapshotHash(nodeId: String, hashes: Set[String])
    extends MetricFailure(s"Node: $nodeId last snapshot hash differs: $hashes")
case class SnapshotDiff(snapshotsToDelete: List[String], snapshotsToDownload: List[String], peers: Set[Id])

object HealthChecker {

  private def choseMajorityState(clusterSnapshots: List[(Id, List[String])]): (List[String], Set[Id]) =
    clusterSnapshots
      .groupBy(_._2)
      .maxBy(_._2.size)
      .map(_.map(_._1).toSet)

  def compareSnapshotState(ownSnapshots: List[String], clusterSnapshots: List[(Id, List[String])]): SnapshotDiff =
    choseMajorityState(clusterSnapshots) match {
      case (snapshots, peers) => SnapshotDiff(ownSnapshots.diff(snapshots), snapshots.diff(ownSnapshots).reverse, peers)
    }

  def checkAllMetrics(apis: Seq[APIClient]): Either[MetricFailure, Unit] = {
    var hashes: Set[String] = Set.empty
    val it = apis.iterator
    var lastCheck: Either[MetricFailure, Unit] = Right(())
    while (it.hasNext && lastCheck.isRight) {
      val a = it.next()
      val metrics = IO.fromFuture(IO { a.metricsAsync })(ConstellationContextShift.edge).unsafeRunSync() // TODO: wkoszycki revisit
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

class HealthChecker[F[_]: Concurrent](
  dao: DAO,
  downloader: DownloadProcess
) extends StrictLogging {

  def checkClusterConsistency(ownSnapshots: List[String]): F[Unit] =
    for {
      _ <- Sync[F].delay { logger.debug(s"[${dao.id.short}] checking cluster consistency") }
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      majoritySnapshots <- LiftIO[F].liftIO(collectSnapshot(peers))
      diff = compareSnapshotState(ownSnapshots, majoritySnapshots)
      _ <- if (diff.snapshotsToDelete.isEmpty && diff.snapshotsToDownload.isEmpty)
        startReDownload(diff, peers.filterKeys(diff.peers.contains))
      else Sync[F].unit
    } yield ()

  def startReDownload(diff: SnapshotDiff, peers: Map[Id, PeerData]): F[Unit] =
    for {
      _ <- Sync[F].delay { logger.debug(s"[${dao.id.short}] starting re-download process $diff") }
      _ <- LiftIO[F].liftIO(downloader.setNodeState(NodeState.DownloadInProgress))
      _ <- LiftIO[F].liftIO(downloader.reDownload(diff.snapshotsToDownload, peers.filterKeys(diff.peers.contains)))
      _ = Snapshot.removeSnapshots(diff.snapshotsToDelete, dao.snapshotPath.pathAsString)
      _ <- Sync[F].delay { logger.debug(s"[${dao.id.short}] re-download process finished") }
    } yield ()

  private def collectSnapshot(peers: Map[Id, PeerData]) =
    peers.toList.traverse(p => (p._1, p._2.client.getNonBlockingIO[List[String]]("snapshot/recent")).sequence)

}
