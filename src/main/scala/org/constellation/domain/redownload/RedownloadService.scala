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
import org.constellation.util.MajorityStateChooser.NodeSnapshots
import org.constellation.util.{HealthChecker, MajorityStateChooser, SnapshotDiff}

import scala.concurrent.duration._

class RedownloadService[F[_]](cluster: Cluster[F], healthChecker: HealthChecker[F])(implicit F: Concurrent[F],
                                                                                    C: ContextShift[F]) {
  val logger = Slf4jLogger.getLogger[F]

  // TODO: Consider Height/Hash type classes
  private[redownload] val proposedSnapshots: Ref[F, Map[Long, Seq[NodeSnapshots]]] = Ref.unsafe(Map.empty)

  def persistLocalSnapshot(recentSnapshot: RecentSnapshot) = updateProposedSnapshots(Seq((cluster.id, recentSnapshot)))

  def getLocalSnapshots() = proposedSnapshots.get.map { snapshots =>
    snapshots.flatMap {
      case (height, nodeSnaps) =>
        val localSnapshotSeq = nodeSnaps.filter { case (id, recentSnapshotSeq) => id == cluster.id }.flatMap {
          case (id, recentSnapshotSeq) => recentSnapshotSeq.map(height -> _)
        }
        localSnapshotSeq
    }
  }

  def getLocalSnapshotAtHeight(height: Long): F[Option[RecentSnapshot]] = proposedSnapshots.get.map { proposedSnaps =>
    val snapshotsAtHeight: Seq[(Id, Seq[RecentSnapshot])] = proposedSnaps.getOrElse(height, Seq())
    val localSnapshotsAtHeight = snapshotsAtHeight.find(_._1 == cluster.id)
    localSnapshotsAtHeight.flatMap { case (id, recentSnapshot) => recentSnapshot.headOption }
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

  private[redownload] def updateProposedSnapshots(fetchedProposals: Seq[(Id, RecentSnapshot)]) = proposedSnapshots.modify { m =>
      val updatedProposals = fetchedProposals.foldLeft(m) {
        case (prevPeerProps, (id, recentSnap)) =>
          val proposalsAtHeight: Seq[(Id, Seq[RecentSnapshot])] = prevPeerProps.getOrElse(recentSnap.height, Seq())
          val hasMadeProposal = proposalsAtHeight.exists(_._1 == id) //todo Observation of new proposal
          if (!hasMadeProposal) prevPeerProps.updated(recentSnap.height, proposalsAtHeight :+ (id, Seq(recentSnap)))
          else prevPeerProps
      }
      (updatedProposals, ())
    }

  def fetchAndSetPeerProposals() = fetchPeerProposals().flatMap(updateProposedSnapshots)

  def recalculateMajoritySnapshot(): F[(Seq[RecentSnapshot], Set[Id])] =
    for {
      allProposals <- proposedSnapshots.get
      allProposalsNormalized = allProposals.values.flatten.toList
      peers <- cluster.readyPeers //todo need testing around join leave, this will cause cluster to stall if cluster.readyPeers not updated when peer leaves
      majority = MajorityStateChooser.chooseMajorWinner(peers.keys.toSeq, allProposalsNormalized)
      groupedProposals = allProposalsNormalized
        .groupBy(_._1)
        .map { case (id, tupList) => (id, tupList.flatMap(_._2)) }
        .toList
      snapsThroughMaj = majority.map(maj => MajorityStateChooser.getAllSnapsUntilMaj(maj._2, groupedProposals))
      majNodeIds = majority.flatMap(maj => MajorityStateChooser.chooseMajNodeIds(maj._2, allProposalsNormalized))
      (sortedSnaps, nodeIdsWithSnaps) = (snapsThroughMaj.map(_.sortBy(-_.height)).getOrElse(Seq()),
                                         majNodeIds.map(_.toSet).getOrElse(Set()))
    } yield (sortedSnaps, nodeIdsWithSnaps)

  def checkForAlignmentWithMajoritySnapshot(): F[Option[List[RecentSnapshot]]] =
    for {
      peers <- cluster.readyPeers
      majSnapsIds <- recalculateMajoritySnapshot()
      ownSnaps <- proposedSnapshots.get
      allOwnSnaps = ownSnaps.values.flatMap(snapsAtHeight => snapsAtHeight.filter(_._1 == cluster.id).map(_._2)).flatten
      diff = MajorityStateChooser.compareSnapshotState(majSnapsIds, allOwnSnaps.toList)
      shouldRedownload = RedownloadService.shouldReDownload(allOwnSnaps.toList, diff)
      result <- if (shouldRedownload) {
        logger.info(
          s"[${cluster.id}] Re-download process with : \n" +
            s"Snapshot to download : ${diff.snapshotsToDownload.map(a => (a.height, a.hash))} \n" +
            s"Snapshot to delete : ${diff.snapshotsToDelete.map(a => (a.height, a.hash))} \n" +
            s"From peers : ${diff.peers} \n" +
            s"Own snapshots : ${allOwnSnaps.map(a => (a.height, a.hash))} \n" +
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
