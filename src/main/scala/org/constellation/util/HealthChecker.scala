package org.constellation.util

import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.chrisdavenport.log4cats.Logger
import org.constellation.consensus.{ConsensusManager, Snapshot}
import org.constellation.p2p.{DownloadProcess, PeerData}
import org.constellation.primitives.ConcurrentTipService
import org.constellation.primitives.Schema.{Id, NodeState, NodeType}
import org.constellation.storage._
import org.constellation.util.HealthChecker.{compareSnapshotState, maxOrZero}
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

  private[util] def choseMajorityState(
    clusterSnapshots: List[(Id, List[RecentSnapshot])]
  ): (List[RecentSnapshot], Set[Id]) = {
    val majority = clusterSnapshots
      .groupBy(_._2)
      .maxBy(z => (z._2.size, maxOrZero(z._1)))
      .map(_.map(_._1).toSet)
    logger.debug(s"re-download clusterSnapshots: ${clusterSnapshots} and majority: ${majority} ")
    majority
  }

  private[util] def choseMajorityWithRecentSync(
    clusterSnapshots: List[(Id, List[RecentSnapshot])],
    ownHeight: Long
  ): (List[RecentSnapshot], Set[_ <: Id]) = {
    val mostRecentSnapshot = getMostRecentSnapshotAndNodeIds(clusterSnapshots)
    if (mostRecentSnapshot.isEmpty) {
      return (List.empty, Set.empty)
    }

    val recentSnapshotWithNodes =
      if (shouldUseMostRecentSnapshot(mostRecentSnapshot.get, ownHeight)) mostRecentSnapshot
      else chooseSnapshotAndNodeIds(clusterSnapshots)

    if (recentSnapshotWithNodes.isEmpty) {
      return (List.empty, Set.empty)
    }
    val recentSnapshot = recentSnapshotWithNodes.get

    val nodeWithMajorityState = clusterSnapshots.find(f => f._1 == recentSnapshot._2.head)
    nodeWithMajorityState match {
      case Some(value) => (value._2.sortBy(_.height).reverse.dropWhile(_ != recentSnapshot._1), recentSnapshot._2)
      case None        => (List.empty, Set.empty)
    }
  }

  private[util] def getMostRecentSnapshotAndNodeIds(clusterSnapshots: List[(Id, List[RecentSnapshot])]) = {
    val recentSnapshotWithNodes = sortSnapshotsAndNodeIds(clusterSnapshots)

    if (recentSnapshotWithNodes.isEmpty) {
      None
    } else {
      Some(recentSnapshotWithNodes.head._1, recentSnapshotWithNodes.head._2.map(_._1).toSet)
    }
  }

  private[util] def shouldUseMostRecentSnapshot(
    mostRecentSnapshots: (RecentSnapshot, Set[Id]),
    ownHeight: Long
  ): Boolean =
    (mostRecentSnapshots._1.height - ownHeight) > 9

  private[util] def choseMajority(clusterSnapshots: List[(Id, List[RecentSnapshot])]) =
    chooseSnapshotAndNodeIds(clusterSnapshots) match {
      case Some(value) => nodesWithMajorityState(clusterSnapshots, value)
      case None        => (List.empty, Set.empty)
    }

  private[util] def nodesWithMajorityState(
    clusterSnapshots: List[(Id, List[RecentSnapshot])],
    node: (RecentSnapshot, Set[Id])
  ) =
    clusterSnapshots.find(f => f._1 == node._2.head) match {
      case Some(value) => (value._2.sortBy(_.height).reverse.dropWhile(_ != node._1), node._2)
      case None        => (List.empty, Set.empty)
    }

  private[util] def dropToCurrentState(snapshot: (Id, List[RecentSnapshot]), majorityState: RecentSnapshot) =
    (snapshot._2.dropWhile(_ != majorityState), snapshot._2)

  private[util] def chooseSnapshotAndNodeIds(clusterSnapshots: List[(Id, List[RecentSnapshot])]) = {
    val sort = sortSnapshotsAndNodeIds(clusterSnapshots)
      .filter(_._2.lengthCompare(clusterSnapshots.size / 2) > 0)

    if (sort.nonEmpty) {
      Some(sort.head._1, sort.head._2.map(_._1).toSet)
    } else {
      None
    }
  }

  private[util] def sortSnapshotsAndNodeIds(clusterSnapshots: List[(Id, List[RecentSnapshot])]) =
    clusterSnapshots
      .flatMap(c => c._2.map((c._1, _)))
      .groupBy(_._2)
      .toList
      .sortBy(_._1.height)
      .reverse

  private def maxOrZero(list: List[RecentSnapshot]): Long =
    list match {
      case Nil      => 0
      case nonEmpty => nonEmpty.map(_.height).max
    }

  def compareSnapshotState(
    ownSnapshots: (Id, List[RecentSnapshot]),
    clusterSnapshots: List[(Id, List[RecentSnapshot])]
  ): SnapshotDiff =
    choseMajorityWithRecentSync(clusterSnapshots :+ ownSnapshots, maxOrZero(ownSnapshots._2)) match {
      case (snapshots, peers) =>
        logger.debug(s"Selected major state : $snapshots")
        SnapshotDiff(
          ownSnapshots._2.diff(snapshots).sortBy(_.height).reverse,
          snapshots.diff(ownSnapshots._2).sortBy(_.height).reverse,
          peers.toList
        )
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
  downloader: DownloadProcess
) extends StrictLogging {

  def checkClusterConsistency(ownSnapshots: List[RecentSnapshot]): F[Option[List[RecentSnapshot]]] = {
    val check = for {
      _ <- Logger[F].debug(s"[${dao.id.short}] re-download checking cluster consistency")
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      peersSnapshots <- collectSnapshot(peers)
      _ <- clearStaleTips(peersSnapshots)
      diff = compareSnapshotState((dao.id, ownSnapshots), peersSnapshots)
      _ <- Logger[F].debug(s"[${dao.id.short}] re-download cluster diff $diff and own $ownSnapshots")
      result <- if (shouldReDownload(ownSnapshots, diff)) {
        startReDownload(diff, peers.filterKeys(diff.peers.contains))
          .flatMap(
            _ =>
              Sync[F]
                .delay[Option[List[RecentSnapshot]]](
                  Some(HealthChecker.choseMajorityWithRecentSync(peersSnapshots, maxOrZero(ownSnapshots))._1)
                )
          )
      } else {
        Sync[F].pure[Option[List[RecentSnapshot]]](None)
      }
    } yield result

    check.recoverWith {
      case err =>
        Logger[F]
          .error(err)(s"Unexpected error during re-download process: ${err.getMessage}")
          .flatMap(_ => Sync[F].pure[Option[List[RecentSnapshot]]](None))
    }
  }

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
      ) *> Sync[F].unit
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

  def startReDownload(diff: SnapshotDiff, peers: Map[Id, PeerData]): F[Unit] = {
    val reDownload = for {
      _ <- Logger[F].debug(s"[${dao.id.short}] starting re-download process with diff: $diff")
      _ <- LiftIO[F].liftIO(downloader.setNodeState(NodeState.DownloadInProgress))
      _ <- consensusManager.terminateConsensuses()
      _ <- LiftIO[F].liftIO(
        downloader.reDownload(
          diff.snapshotsToDownload.map(_.hash).filterNot(_ == Snapshot.snapshotZeroHash),
          peers.filterKeys(diff.peers.contains)
        )
      )
      _ <- Sync[F].delay {
        Snapshot.removeSnapshots(
          diff.snapshotsToDelete.map(_.hash).filterNot(_ == Snapshot.snapshotZeroHash),
          dao.snapshotPath.pathAsString
        )(dao)
      }
      _ <- Logger[F].debug(s"[${dao.id.short}] re-download process finished")
      _ <- dao.metrics.incrementMetricAsync(Metrics.reDownloadFinished)
    } yield ()

    reDownload.recoverWith {
      case err =>
        for {
          _ <- LiftIO[F].liftIO(downloader.setNodeState(NodeState.Ready))
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
