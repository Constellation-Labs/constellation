package org.constellation.domain.redownload

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, LiftIO, Sync}
import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.consensus.EdgeProcessor
import org.constellation.domain.snapshotInfo.SnapshotInfoChunk
import org.constellation.schema.Id
import org.constellation.storage.RecentSnapshot
import org.constellation.util.MajorityStateChooser.NodeSnapshots
import org.constellation.util.{HealthChecker, MajorityStateChooser}

import scala.concurrent.duration._

class RedownloadService[F[_]](dao: DAO)(implicit F: Concurrent[F], C: ContextShift[F]) {
  val logger = Slf4jLogger.getLogger[F]

  // TODO: Consider Height/Hash type classes
  private[redownload] val ownSnapshots: Ref[F, Map[Long, RecentSnapshot]] = Ref.unsafe(Map.empty)
  private[redownload] val peersProposals: Ref[F, Map[Long, Seq[NodeSnapshots]]] = Ref.unsafe(Map.empty)//todo join/leave?

  def persistOwnSnapshot(height: Long, recentSnapshot: RecentSnapshot): F[Unit] =
    ownSnapshots.modify { m =>
      val updated = if (m.get(height).nonEmpty) m else m.updated(height, recentSnapshot)
      (updated, ())
    }

  def getOwnSnapshots(): F[Map[Long, RecentSnapshot]] = ownSnapshots.get

  def getOwnSnapshot(height: Long): F[Option[RecentSnapshot]] =
    ownSnapshots.get.map(_.get(height))

  def getProposals() =
    for {
      peers <- LiftIO[F].liftIO(dao.readyPeers)
      chunkedPeerProposalMaps <- peers.toList.traverse {
        case (id, peerData) =>
          val resp = peerData.client.getNonBlockingFLogged[F, Array[Array[Byte]]](
            "snapshot/own",
            timeout = 45.seconds, //todo change these?
            tag = "snapshot/obj/snapshot"
          )(C)
          resp.map((id, _))
      }
    } yield chunkedPeerProposalMaps

  def updatePeerProps(fetchedProposals: Seq[(Id, RecentSnapshot)]) =
    peersProposals.modify { m =>
      val updatedProposals = fetchedProposals.foldLeft(m) {
        case (prevPeerProps, (id, recentSnap)) =>
          val proposalsAtHeight: Seq[(Id, Seq[RecentSnapshot])] = prevPeerProps.getOrElse(recentSnap.height, Seq())
          val hasMadeProposal = proposalsAtHeight.exists(_._1 == id) //todo Observation of new proposal, dupes ok for now
          if (!hasMadeProposal) prevPeerProps.updated(recentSnap.height, proposalsAtHeight :+ (id, Seq(recentSnap)))
          else prevPeerProps
      }
      (updatedProposals, updatedProposals)
    }

  // TODO: Use cluster peers to fetch "/snapshot/own" and merge with peersProposals Map
  def fetchAndSetPeersProposals() = {
    val fetchedProposals = getProposals().map { proposals =>
      proposals.map(RedownloadService.deSerProps).flatMap {
        case (id, recentSnaps) => recentSnaps.map(snap => (id, snap))
      }
    }
    fetchedProposals.flatMap(updatePeerProps)
  }



//  // TODO: Check for the majority snapshot and store under a variable
//  def recalculateMajoritySnapshot(): F[(Seq[RecentSnapshot], Set[Id])] = for {
//      peerProps <- peersProposals.get
//      ownProps <- ownSnapshots.get
//      ownPropsNormalized = ownProps.mapValues(snap => List((dao.id, Seq(snap))))
//      allProposals = (peerProps.mapValues(_.toList) |+| ownPropsNormalized).flatMap(_._2.toSeq).toList
//      peers <- LiftIO[F].liftIO(dao.readyPeers) //todo need testing around join leave, this could cause divergence
//      majority = MajorityStateChooser.chooseMajorWinner(peers.keys.toSeq, allProposals)
//      snapsThroughMaj = majority.flatMap(maj => MajorityStateChooser.getAllSnapsUntilMaj(maj._2, allProposals))
//      majNodeIds = majority.flatMap(maj => MajorityStateChooser.chooseMajNodeIds(maj._2, allProposals))
//    } yield (snapsThroughMaj.map(_.sortBy(-_.height)).getOrElse(Seq()), majNodeIds.map(_.toSet).getOrElse(Set()))

  // TODO: Check for the majority snapshot and store under a variable
  def recalculateMajoritySnapshot(): F[(Seq[RecentSnapshot], Set[Id])] = {
    val maps = for {
      peerProps <- peersProposals.get
      ownProps <- ownSnapshots.get
      ownPropsNormalized = ownProps.mapValues(snap => List((dao.id, Seq(snap))))
      allProposals = peerProps.mapValues(_.toList) |+| ownPropsNormalized
    } yield allProposals

    for {
      props <- maps
      test = props.values.flatten.toList
      allProposals = props.values.flatten.toList//props.flatMap(_._2.toSeq).toList
      peers <- LiftIO[F].liftIO(dao.readyPeers) //todo need testing around join leave, this could cause divergence
      //todo bubug here ^ only sees one peer
      majority = MajorityStateChooser.chooseMajorWinner(peers.keys.toSeq, allProposals)
      groupedProposals = allProposals.toSeq.groupBy(_._1).map{ case (id, tupList) => (id, tupList.flatMap(_._2))}.toList
      snapsThroughMaj = majority.map(maj => MajorityStateChooser.getAllSnapsUntilMaj(maj._2, groupedProposals))
      //todo bubug here ^ only sees one peer
      majNodeIds = majority.flatMap(maj => MajorityStateChooser.chooseMajNodeIds(maj._2, allProposals))
    } yield (snapsThroughMaj.map(_.sortBy(-_.height)).getOrElse(Seq()), majNodeIds.map(_.toSet).getOrElse(Set()))
  }

  // TODO: Check for the alignment ownSnapshots <-> majoritySnapshot and call redownload if needed
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

  def deSerProps(nodeSnap: (Id, Array[Array[Byte]])): (Id, Seq[RecentSnapshot]) = nodeSnap match {
    case (id, serializedProposalMap) =>
      val proposals: Seq[RecentSnapshot] = serializedProposalMap.flatMap { chunk =>
        EdgeProcessor
          .chunkDeSerialize[Seq[(Long, RecentSnapshot)]](chunk, SnapshotInfoChunk.SNAPSHOT_FETCH_PROPOSALS.name)
          .map(_._2)
      }.toSeq
      val nodeSnapshots = (id, proposals)
      nodeSnapshots
  }

  def apply[F[_]: Concurrent](dao: DAO)(implicit C: ContextShift[F]): RedownloadService[F] =
    new RedownloadService[F](dao)
}
