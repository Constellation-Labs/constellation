package org.constellation.domain.redownload

import cats.effect.{ContextShift, LiftIO, Sync}
import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.serializer.KryoSerializer
import org.constellation.schema.Id
import org.constellation.storage.RecentSnapshot
import org.constellation.util.MajorityStateChooser.NodeSnapshots
import org.constellation.util.{HealthChecker, MajorityStateChooser}

import scala.concurrent.duration._

class RedownloadService[F[_]](dao: DAO)(implicit F: Concurrent[F], C: ContextShift[F]) {
  val logger = Slf4jLogger.getLogger[F]

  // TODO: Consider Height/Hash type classes
  private[redownload] val ownSnapshots: Ref[F, Map[Long, RecentSnapshot]] = Ref.unsafe(Map.empty)
  private[redownload] val peersProposals
    : Ref[F, Map[Long, Seq[NodeSnapshots]]] = Ref.unsafe(Map.empty) //todo join/leave?

  def persistOwnSnapshot(recentSnapshot: RecentSnapshot): F[Unit] =
    ownSnapshots.modify { m =>
      val updated = if (m.get(recentSnapshot.height).nonEmpty) m else m.updated(recentSnapshot.height, recentSnapshot)
      (updated, ())
    }

  def getOwnSnapshots(): F[Map[Long, RecentSnapshot]] = ownSnapshots.get

  def getOwnSnapshot(height: Long): F[Option[RecentSnapshot]] =
    ownSnapshots.get.map(_.get(height))

  private[redownload] def getProposals() =
    for {
      peers <- LiftIO[F].liftIO(dao.readyPeers)
      chunkedPeerProposalMaps <- peers.toList.traverse {
        case (id, peerData) =>
          val resp = peerData.client.getNonBlockingFLogged[F, Array[Array[Byte]]](
            "snapshot/own",
            timeout = 45.seconds, //todo change these?
            tag = "snapshot/own"
          )(C)
          resp.map((id, _))
      }
    } yield chunkedPeerProposalMaps

  def updatePeerProposals(fetchedProposals: Seq[(Id, RecentSnapshot)]) =
    peersProposals.modify { m =>
      val updatedProposals = fetchedProposals.foldLeft(m) {
        case (prevPeerProps, (id, recentSnap)) =>
          val proposalsAtHeight: Seq[(Id, Seq[RecentSnapshot])] = prevPeerProps.getOrElse(recentSnap.height, Seq())
          val hasMadeProposal = proposalsAtHeight.exists(_._1 == id) //todo Observation of new proposal
          if (!hasMadeProposal) prevPeerProps.updated(recentSnap.height, proposalsAtHeight :+ (id, Seq(recentSnap)))
          else prevPeerProps
      }
      (updatedProposals, updatedProposals)
    }

  def fetchAndSetPeersProposals() = {
    val fetchedProposals = getProposals().map { proposals =>
      proposals.map(RedownloadService.deserializeProposals).flatMap {
        case (id, recentSnaps) => recentSnaps.map(snap => (id, snap))
      }
    }
    fetchedProposals.flatMap(updatePeerProposals)
  }

  def recalculateMajoritySnapshot(): F[(Seq[RecentSnapshot], Set[Id])] =
    for {
      peerProps <- peersProposals.get
      ownProps <- ownSnapshots.get
      ownPropsNormalized = ownProps.mapValues(snap => List((dao.id, Seq(snap))))
      allProposals = peerProps.mapValues(_.toList) |+| ownPropsNormalized
      allProposalsNormalized = allProposals.values.flatten.toList
      peers <- LiftIO[F].liftIO(dao.readyPeers) //todo need testing around join leave, this could cause divergence
      majority = MajorityStateChooser.chooseMajorWinner(peers.keys.toSeq, allProposalsNormalized)
      groupedProposals = allProposalsNormalized
        .groupBy(_._1)
        .map { case (id, tupList) => (id, tupList.flatMap(_._2)) }
        .toList
      snapsThroughMaj = majority.map(maj => MajorityStateChooser.getAllSnapsUntilMaj(maj._2, groupedProposals))
      majNodeIds = majority.flatMap(maj => MajorityStateChooser.chooseMajNodeIds(maj._2, allProposalsNormalized))
      (sortedSnaps, nodeIdsWithSnaps) = (snapsThroughMaj.map(_.sortBy(-_.height)).getOrElse(Seq()), majNodeIds.map(_.toSet).getOrElse(Set()))
    } yield (sortedSnaps, nodeIdsWithSnaps)

  def checkForAlignmentWithMajoritySnapshot(): F[Option[List[RecentSnapshot]]] =
    for {
      peers <- LiftIO[F].liftIO(dao.readyPeers)
      majSnapsIds <- recalculateMajoritySnapshot()
      ownSnaps <- ownSnapshots.get
      diff = HealthChecker.compareSnapshotState(majSnapsIds, ownSnaps.values.toList)
      shouldRedownload = HealthChecker.shouldReDownload(ownSnaps.values.toList, diff)
      result <- if (shouldRedownload) {
        logger.info(
          s"[${dao.id.short}] Re-download process with : \n" +
            s"Snapshot to download : ${diff.snapshotsToDownload.map(a => (a.height, a.hash))} \n" +
            s"Snapshot to delete : ${diff.snapshotsToDelete.map(a => (a.height, a.hash))} \n" +
            s"From peers : ${diff.peers} \n" +
            s"Own snapshots : ${ownSnaps.values.map(a => (a.height, a.hash))} \n" +
            s"Major state : $majSnapsIds"
        ) >>
          LiftIO[F]
            .liftIO(dao.healthChecker.startReDownload(diff, peers.filterKeys(diff.peers.contains)))
            .flatMap(_ => Sync[F].delay[Option[List[RecentSnapshot]]](Some(majSnapsIds._1.toList)))
      } else {
        Sync[F].pure[Option[List[RecentSnapshot]]](None)
      }
    } yield result

}

object RedownloadService {

  def deserializeProposals(nodeSnap: (Id, Array[Array[Byte]])): (Id, Seq[RecentSnapshot]) = nodeSnap match {
    case (id, serializedProposalMap) =>
      val proposals: Seq[RecentSnapshot] = serializedProposalMap.flatMap { chunk =>
        KryoSerializer
          .chunkDeSerialize[Seq[(Long, RecentSnapshot)]](chunk, "fetchSnapshotProposals")
          .map(_._2)
      }.toSeq
      val nodeSnapshots = (id, proposals)
      nodeSnapshots
  }

  def apply[F[_]: Concurrent](dao: DAO)(implicit C: ContextShift[F]): RedownloadService[F] =
    new RedownloadService[F](dao)
}
