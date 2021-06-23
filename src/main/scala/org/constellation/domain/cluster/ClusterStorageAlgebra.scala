package org.constellation.domain.cluster

import org.constellation.p2p.{JoinedHeight, PeerData}
import org.constellation.schema.{Id, NodeState}

trait ClusterStorageAlgebra[F[_]] {

  def getPeer(id: Id): F[Option[PeerData]]
  def getPeer(host: String): F[Option[PeerData]]
  def registerPeer(peerData: PeerData): F[Unit]
  def updatePeer(peerData: PeerData): F[Unit]
  def removePeer(id: Id): F[Unit]
  def removePeer(peerData: PeerData): F[Unit]

  def getPeers: F[Map[Id, PeerData]]
  def getReadyPeers: F[Map[Id, PeerData]]
  def getJoinedPeers: F[Map[Id, PeerData]]
  def getReadyAndFullPeers: F[Map[Id, PeerData]]
  def getLeavingPeers: F[Map[Id, PeerData]]
  def getNotOfflinePeers: F[Map[Id, PeerData]]
  def clearPeers(): F[Unit]

  def updateJoinedHeight(joinedHeight: JoinedHeight): F[Unit]

  def setNodeState(id: Id, state: NodeState): F[Unit]

}
