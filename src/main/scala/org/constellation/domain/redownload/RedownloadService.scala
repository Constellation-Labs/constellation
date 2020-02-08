package org.constellation.domain.redownload

import cats.effect.{Clock, Concurrent, ContextShift, IO, LiftIO, Timer}
import cats.effect.concurrent.Ref
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.primitives.Schema.NodeType
import org.constellation.schema.Id
import org.constellation.storage.RecentSnapshot
import org.constellation.util.MajorityStateChooser
import org.constellation.util.MajorityStateChooser.NodeSnapshots
import org.constellation.consensus.EdgeProcessor
import org.constellation.domain.snapshotInfo.SnapshotInfoChunk

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

class RedownloadService[F[_]](dao: DAO)(implicit F: Concurrent[F], C: ContextShift[F]) {

  // TODO: Consider Height/Hash type classes
  private[redownload] val ownSnapshots: Ref[F, Map[Long, RecentSnapshot]] = Ref.unsafe(Map.empty)
  private[redownload] val peersProposals: Ref[F, Map[Long, Seq[NodeSnapshots]]] = Ref.unsafe(Map.empty)//type NodeSnapshots = (Id, Seq[RecentSnapshot])//todo join/leave?

  def persistOwnSnapshot(height: Long, recentSnapshot: RecentSnapshot): F[Unit] =
    ownSnapshots.modify { m =>
      val updated = if (m.get(height).nonEmpty) m else m.updated(height, recentSnapshot)
      (updated, ())
    }

  def getOwnSnapshots(): F[Map[Long, RecentSnapshot]] = ownSnapshots.get

  def getOwnSnapshot(height: Long): F[Option[RecentSnapshot]] =
    ownSnapshots.get.map(_.get(height))

  // TODO: Use cluster peers to fetch "/snapshot/own" and merge with peersProposals Map
  def fetchPeersProposals(): F[Unit] = ???

  def fetchPeersProposalsImpl() = {
    val responses: F[Seq[NodeSnapshots]] = for {
      peers <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Full))
      chunkedPeerProposalMaps <- peers.toList.traverse { case (id, peerData) =>
        val resp = peerData.client.getNonBlockingFLogged[F, Array[Array[Byte]]](
          "snapshot/own",
          timeout = 45.seconds, //todo change these?
          tag = "snapshot/obj/snapshot"
        )(C)
        resp.map((id, _))
      }
    } yield chunkedPeerProposalMaps.map {
      case (id, serializedProposalMap) =>
        val proposals: Seq[RecentSnapshot] = serializedProposalMap.flatMap { chunk =>
          EdgeProcessor.chunkDeSerialize[Seq[(Long, RecentSnapshot)]](chunk, SnapshotInfoChunk.SNAPSHOT_FETCH_PROPOSALS.name).map(_._2)
        }.toSeq
        val nodeSnapshots = (id, proposals)
        nodeSnapshots
    }

    val updatePeersProposals = for {
      nodeSnapshotsSeq <- responses
    } yield peersProposals.modify { m =>
      var updated = m
      nodeSnapshotsSeq.foreach { case (id, recentSnaps: Seq[RecentSnapshot]) =>
        recentSnaps.foreach{
          recentSnap: RecentSnapshot =>
            val proposalsAtHeight: Option[Seq[(Id, Seq[RecentSnapshot])]] = m.get(recentSnap.height)
            val hasPrevProposal: Boolean = proposalsAtHeight.map(_.exists(prop => prop._1 == id)).contains(true)
            val hasSameProposal: Boolean = !proposalsAtHeight.exists(_.contains(recentSnap))
            if (!hasSameProposal && !hasPrevProposal){//todo log case for .find(prop => prop._1 == id
              val props: Seq[(Id, Seq[RecentSnapshot])] = proposalsAtHeight.get//todo node right in props should be size 1
              updated = updated.updated(recentSnap.height, props :+ (id, Seq(recentSnap)))
            }
        }
      }
      (updated, ())
    }
    updatePeersProposals
  }

  // TODO: Check for the majority snapshot and store under a variable
  def recalculateMajoritySnapshot(): F[Unit] = peersProposals.get.map { proposals: Map[Long, Seq[(Id, Seq[RecentSnapshot])]] =>
    val allPeers: F[Iterable[Id]] = for {
      peers <- LiftIO[F].liftIO(dao.peerInfo)
    } yield peers.keys
    val allProposals: List[(Id, Seq[RecentSnapshot])] = proposals.values.flatten.toList
    val majorState: F[Option[(Long, (RecentSnapshot, Seq[Id]))]] = allPeers.map(peers => MajorityStateChooser.chooseMajorWinner(peers.toSeq, allProposals))
    val snapsThoughMaj: F[Option[Seq[RecentSnapshot]]] = majorState.map{
      case Some(majState) =>
        val snapsUntilMaj: Option[Seq[RecentSnapshot]] = MajorityStateChooser.getAllSnapsUntilMaj(majState._2, allProposals)
        snapsUntilMaj
    }
        val nodeIds = majorState.map{
          case Some(state) =>
            val majNodeIds = MajorityStateChooser.chooseMajNodeIds(state._2, allProposals)
            majNodeIds
        }
          val res = for {
            snaps <- snapsThoughMaj
            ids <- nodeIds
          } yield (snaps.map(_.sortBy(-_.height)), ids.toSet)
    res
  }

  // TODO: Check for the alignment ownSnapshots <-> majoritySnapshot and call redownload if needed
  def checkForAlignmentWithMajoritySnapshot(): F[Unit] = ???

}

object RedownloadService {
  def apply[F[_]: Concurrent](dao: DAO)(implicit C: ContextShift[F]): RedownloadService[F] = new RedownloadService[F](dao)
}
