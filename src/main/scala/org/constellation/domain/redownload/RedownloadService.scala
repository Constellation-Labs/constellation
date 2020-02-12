package org.constellation.domain.redownload

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil
import org.constellation.p2p.Cluster
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.storage.RecentSnapshot
import org.constellation.util.{HealthChecker, MajorityStateChooser, SnapshotDiff}

import scala.concurrent.duration._

class RedownloadService[F[_]](cluster: Cluster[F], healthChecker: HealthChecker[F])(implicit F: Concurrent[F],
                                                                                    C: ContextShift[F]) {
  type Proposals = Map[Id, RecentSnapshot]
  val logger = Slf4jLogger.getLogger[F]

  // TODO: Consider Height/Hash type classes
  private[redownload] val proposedSnapshots: Ref[F, Map[Long, Proposals]] = Ref.unsafe(Map.empty)

  def persistLocalSnapshot(recentSnapshot: RecentSnapshot) = updateProposedSnapshots(Seq((cluster.id, recentSnapshot)))

  def getLocalSnapshots() = proposedSnapshots.get.map { snapshots =>
    snapshots.values.flatMap { proposalsForHeight =>
      proposalsForHeight.get(cluster.id)
    }.toSeq
  }

  def getLocalSnapshotAtHeight(height: Long): F[Option[RecentSnapshot]] = proposedSnapshots.get.map { proposedSnaps =>
    val snapshotsAtHeight = proposedSnaps.getOrElse(height, Map())
    snapshotsAtHeight.get(cluster.id)
  }

  private[redownload] def fetchPeerProposals(): F[List[(Id, RecentSnapshot)]] =
    for {
      peers <- cluster.readyPeers
      chunkedPeerProposals <- peers.toList.traverse {
        case (id, peerData) =>
          val resp = peerData.client.getNonBlockingFLogged[F, Array[Array[Byte]]](
            "snapshot/own",
            timeout = 3.seconds,
            tag = "snapshot/own"
          )(C)
          resp.map((id, _)).handleErrorWith { ex =>
            Sync[F].delay(logger.error(s"fetchPeerProposals error - ${ex}")) >> Sync[F].delay((id, Array()))
          }
      }
      deSerializedPeerProposals = chunkedPeerProposals.map(RedownloadService.deserializeProposals).flatMap {
        case (id, recentSnaps) => recentSnaps.map(snap => (id, snap))
      }
    } yield deSerializedPeerProposals

  private[redownload] def updateProposedSnapshots(fetchedProposals: Seq[(Id, RecentSnapshot)]) =
    proposedSnapshots.modify { m =>
      val updatedProposals = fetchedProposals.foldLeft(m) {
        case (prevProposals, (id, recentSnap)) =>
          val proposalsAtHeight = prevProposals.getOrElse(recentSnap.height, Map())
          val hasMadeProposal = proposalsAtHeight.get(id).isDefined //todo Observation of new proposal
          val test = prevProposals.updated(recentSnap.height, proposalsAtHeight + (id -> recentSnap))
          if (!hasMadeProposal) test
          else prevProposals
      }
      (updatedProposals, ())
    }

  def fetchAndSetPeerProposals() = fetchPeerProposals().flatMap(updateProposedSnapshots)

  def recalculateMajoritySnapshot(): F[(Seq[RecentSnapshot], Set[Id])] =
    for {
      allProposals <- proposedSnapshots.get
      allProposalsNormalized = allProposals.toSeq.flatMap {
        case (height, proposals: Map[Id, RecentSnapshot]) =>
          proposals.map { case (id, recentSnapshot) => (id, Seq(recentSnapshot)) }.toSeq
      }
      peers <- cluster.readyPeers //todo need testing around join leave, this will cause cluster to stall if cluster.readyPeers not updated when peer leaves
      (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(allProposalsNormalized, peers.keySet.toList)
    } yield (sortedSnaps, nodeIdsWithSnaps)

  def checkForAlignmentWithMajoritySnapshot(): F[Option[List[RecentSnapshot]]] =
    for {
      peers <- cluster.readyPeers
      majSnapsIds <- recalculateMajoritySnapshot()
      ownSnaps <- getLocalSnapshots()
      diff = MajorityStateChooser.compareSnapshotState(majSnapsIds, ownSnaps.toList)
      shouldRedownload = RedownloadService.shouldReDownload(ownSnaps.toList, diff)
      result <- if (shouldRedownload) {
        logger.info(
          s"[${cluster.id}] Re-download process with : \n" +
            s"Snapshot to download : ${diff.snapshotsToDownload.map(a => (a.height, a.hash))} \n" +
            s"Snapshot to delete : ${diff.snapshotsToDelete.map(a => (a.height, a.hash))} \n" +
            s"From peers : ${diff.peers} \n" +
            s"Own snapshots : ${ownSnaps.map(a => (a.height, a.hash))} \n" +
            s"Major state : $majSnapsIds"
        ) >>
          healthChecker
            .startReDownload(diff, peers.filterKeys(diff.peers.contains))
            .flatMap(_ => Sync[F].delay[Option[List[RecentSnapshot]]](Some(majSnapsIds._1.toList)))
      } else {
        Sync[F].pure[Option[List[RecentSnapshot]]](None)
      }
    } yield result
}

object RedownloadService {
  val snapshotHeightDelayInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightDelayInterval")
  val fetchSnapshotProposals = "fetchSnapshotProposals"

  val snapshotHeightRedownloadDelayInterval: Int =
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval")

  private def isMisaligned(ownSnapshots: List[RecentSnapshot], recent: Map[Long, String]) =
    ownSnapshots.exists(r => recent.get(r.height).exists(_ != r.hash))

  private def isAboveInterval(ownSnapshots: List[RecentSnapshot], snapshotsToDownload: List[RecentSnapshot]) =
    (maxOrZero(ownSnapshots) - snapshotHeightDelayInterval) > maxOrZero(
      snapshotsToDownload
    )

  private def isBelowInterval(ownSnapshots: List[RecentSnapshot], snapshotsToDownload: List[RecentSnapshot]) =
    (maxOrZero(ownSnapshots) + snapshotHeightDelayInterval) < maxOrZero(
      snapshotsToDownload
    )

  def shouldReDownload(ownSnapshots: List[RecentSnapshot], diff: SnapshotDiff): Boolean =
    diff match {
      case SnapshotDiff(_, _, Nil) => false
      case SnapshotDiff(_, Nil, _) => false
      case SnapshotDiff(snapshotsToDelete, snapshotsToDownload, _) =>
        val above = isAboveInterval(ownSnapshots, snapshotsToDownload)
        val below = isBelowInterval(ownSnapshots, snapshotsToDownload)
        val misaligned =
          isMisaligned(ownSnapshots, (snapshotsToDelete ++ snapshotsToDownload).map(r => (r.height, r.hash)).toMap)
        above || below || misaligned
    }

  def maxOrZero(list: List[RecentSnapshot]): Long =
    list match {
      case Nil      => 0
      case nonEmpty => nonEmpty.map(_.height).max
    }

  def deserializeProposals(nodeSnap: (Id, Array[Array[Byte]])): (Id, Seq[RecentSnapshot]) = nodeSnap match {
    case (id, serializedProposalMap) =>
      val proposals: Seq[RecentSnapshot] = serializedProposalMap.flatMap { chunk =>
        KryoSerializer
          .chunkDeSerialize[Seq[(Long, RecentSnapshot)]](chunk, fetchSnapshotProposals)
          .map(_._2)
      }.toSeq
      val nodeSnapshots = (id, proposals)
      nodeSnapshots
  }

  def apply[F[_]: Concurrent](cluster: Cluster[F],
                              healthChecker: HealthChecker[F])(implicit C: ContextShift[F]): RedownloadService[F] =
    new RedownloadService[F](cluster, healthChecker)
}
