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
  import RedownloadService._
  val logger = Slf4jLogger.getLogger[F]

  // TODO: Consider Height/Hash type classes
  private[redownload] val proposedSnapshots: Ref[F, Map[Id, Proposals]] = Ref.unsafe(Map.empty)

  def persistLocalSnapshot(recentSnapshot: RecentSnapshot) = updateProposedSnapshots(List((cluster.id, Map(recentSnapshot.height -> recentSnapshot))))

  def getLocalSnapshots() = proposedSnapshots.get.map { snapshots =>
    snapshots.getOrElse(cluster.id, Map())
  }

  def getLocalSnapshotAtHeight(height: Long): F[Option[RecentSnapshot]] = proposedSnapshots.get.map { proposedSnaps =>
    val localSnapshots = proposedSnaps.getOrElse(cluster.id, Map())
    localSnapshots.get(height)
  }

  private[redownload] def fetchPeerProposals(): F[List[(Id, Proposals)]] =
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
      deSerializedPeerProposals = chunkedPeerProposals.map(deserializeProposals).flatMap {
        case (id, recentSnaps) =>
          val heightRecentSnapMap = recentSnaps.map { recentSnap => (recentSnap.height, recentSnap)}.toMap
          recentSnaps.map(snap => (id, heightRecentSnapMap))
      }
    } yield deSerializedPeerProposals

  private[redownload] def updateProposedSnapshots(fetchedProposals: List[(Id, Proposals)]) =
    proposedSnapshots.modify { m =>
      val updatedProposals = fetchedProposals.foldLeft(m) {
        case (prevProposals, (id, proposals)) =>
          val proposalsRecievedFromId = prevProposals.get(id).fold(proposals) { prevProposals =>
            val newProposals = proposals.filter { case (existingHeight, existingSnap) => prevProposals.get(existingHeight).isEmpty }
            prevProposals ++ newProposals
          }
          prevProposals.updated(id, proposalsRecievedFromId)
      }
      (updatedProposals, ())
    }

  def fetchAndSetPeerProposals() = fetchPeerProposals().flatMap(updateProposedSnapshots)

  def recalculateMajoritySnapshot(): F[(Seq[RecentSnapshot], Set[Id])] =
    for {
      allPeers <- cluster.getPeerInfo
      allProposals <- proposedSnapshots.get
      allProposalsNormalized = allProposals.toSeq.flatMap {
        case (id, proposals: Map[Long, RecentSnapshot]) =>
          proposals.toSeq.map { case (height, recentSnapshot) => (id, Seq(recentSnapshot)) }
      }
      (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(allProposalsNormalized, allPeers.keySet.toList)
    } yield (sortedSnaps, nodeIdsWithSnaps)

  def checkForAlignmentWithMajoritySnapshot(): F[Option[List[RecentSnapshot]]] =
    for {
      peers <- cluster.readyPeers
      majSnapsIds <- recalculateMajoritySnapshot()
      ownSnaps <- getLocalSnapshots()
      diff = MajorityStateChooser.compareSnapshotState(majSnapsIds, ownSnaps.values.toList)
      shouldRedownload = shouldReDownload(ownSnaps.values.toList, diff)
      result <- if (shouldRedownload) {
        logger.info(
          s"[${cluster.id}] Re-download process with : \n" +
            s"Snapshot to download : ${diff.snapshotsToDownload.map(a => (a.height, a.hash))} \n" +
            s"Snapshot to delete : ${diff.snapshotsToDelete.map(a => (a.height, a.hash))} \n" +
            s"From peers : ${diff.peers} \n" +
            s"Own snapshots : ${ownSnaps.values.map(a => (a.height, a.hash)).toList} \n" +
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
  type Proposals = Map[Long, RecentSnapshot]

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
