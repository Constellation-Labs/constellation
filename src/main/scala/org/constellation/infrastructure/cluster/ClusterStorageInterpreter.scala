package org.constellation.infrastructure.cluster

import cats.effect.concurrent.Ref
import cats.effect.{Clock, Sync}
import cats.syntax.all._
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.p2p.{JoinedHeight, PeerData}
import org.constellation.schema.{Id, NodeState, NodeType}

import scala.concurrent.duration._

class ClusterStorageInterpreter[F[_]]()(implicit F: Sync[F], C: Clock[F]) extends ClusterStorageAlgebra[F] {

  private val peers: Ref[F, Map[Id, PeerData]] = Ref.unsafe(Map.empty[Id, PeerData])

  def getPeer(host: String): F[Option[PeerData]] =
    getPeers.map(_.values.find {
      _.peerMetadata.host == host
    })

  def registerPeer(peerData: PeerData): F[Unit] =
    peers.modify { p =>
      (p + (peerData.peerMetadata.id -> peerData), ())
    }

  def removePeer(peerData: PeerData): F[Unit] =
    removePeer(peerData.peerMetadata.id)

  def removePeer(id: Id): F[Unit] =
    peers.modify { p =>
      (p - id, ())
    }

  def getLeavingPeers: F[Map[Id, PeerData]] =
    getPeers.map(_.filter(eqNodeState(Set(NodeState.Leaving))))

  def getReadyAndFullPeers: F[Map[Id, PeerData]] =
    getReadyPeers.map(_.filter(eqNodeType(NodeType.Full)))

  def getReadyPeers: F[Map[Id, PeerData]] =
    getPeers.map(_.filter(eqNodeState(NodeState.readyStates)))

  def getPeers: F[Map[Id, PeerData]] = peers.get

  private def eqNodeState(nodeStates: Set[NodeState])(peer: (Id, PeerData)): Boolean =
    nodeStates.contains(peer._2.peerMetadata.nodeState)

  private def eqNodeType(nodeType: NodeType)(peer: (Id, PeerData)): Boolean =
    peer._2.peerMetadata.nodeType == nodeType

  def updateJoinedHeight(joinedHeight: JoinedHeight): F[Unit] =
    getPeer(joinedHeight.id).flatMap { peer =>
      peer.fold(F.unit) { peerData =>
        updatePeer(peerData.updateJoiningHeight(joinedHeight.height))
      }
    }

  def getPeer(id: Id): F[Option[PeerData]] = getPeers.map(_.get(id))

  def updatePeer(peerData: PeerData): F[Unit] =
    peers.modify { p =>
      p.get(peerData.peerMetadata.id) match {
        case Some(_) =>
          (p + (peerData.peerMetadata.id -> peerData), ())
        case None => (p, ())
      }
    }

  def setNodeState(id: Id, nodeState: NodeState): F[Unit] =
    getPeer(id).flatMap { peer =>
      peer.fold(F.unit) { peerData =>
        updatePeer(peerData.copy(peerMetadata = peerData.peerMetadata.copy(nodeState = nodeState)))
      }
    }

  def getNotOfflinePeers: F[Map[Id, PeerData]] =
    getPeers.map {
      _.values.toList.filter(p => NodeState.isNotOffline(p.peerMetadata.nodeState))
    }

  def clearPeers(): F[Unit] =
    peers.modify(_ => (Map.empty, ()))
}
