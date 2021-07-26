package org.constellation.domain.cluster

import org.constellation.p2p.{JoinedHeight, MajorityHeight, PeerData}
import org.constellation.schema.snapshot.NextActiveNodes
import org.constellation.schema.{Id, NodeState, NodeType}
import org.constellation.util.Metrics

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
  def getLeavingPeers: F[Map[Id, PeerData]]
  def getNotOfflinePeers: F[Map[Id, PeerData]]
  def clearPeers(): F[Unit]

  def updateJoinedHeight(joinedHeight: JoinedHeight): F[Unit]

  def setNodeState(id: Id, state: NodeState): F[Unit]

  def getActivePeersIds(withSelfId: Boolean = false): F[Set[Id]]
  def getActivePeers: F[Map[Id, PeerData]]
  def getActiveLightPeersIds(withSelfId: Boolean = false): F[Set[Id]]
  def getActiveLightPeers(): F[Map[Id, PeerData]]
  def getActiveFullPeersIds(withSelfId: Boolean = false): F[Set[Id]]
  def getActiveFullPeers(): F[Map[Id, PeerData]]
  def setActiveFullPeers(peers: Set[Id]): F[Unit]
  def isAnActivePeer: F[Boolean]
  def isAnActiveLightPeer: F[Boolean]
  def isAnActiveFullPeer: F[Boolean]
  def getActiveBetweenHeights: F[MajorityHeight]
  def setActiveBetweenHeights(majorityHeight: MajorityHeight): F[Unit]
  def clearActiveBetweenHeights(): F[Unit]
  def getAuthorizedPeers: F[Set[Id]]
  def setAuthorizedPeers(peers: Set[Id]): F[Unit]
  def addAuthorizedPeer(id: Id): F[Unit]
  def removeAuthorizedPeer(id: Id): F[Unit]
  def getFullPeers(): F[Map[Id, PeerData]]
  def getFullPeersIds(): F[Set[Id]]
  def getLightPeers(): F[Map[Id, PeerData]]
  def getLightPeersIds(): F[Set[Id]]
  def setAsActivePeer(asType: NodeType): F[Unit]
  def unsetAsActivePeer(): F[Unit]
  def setActivePeers(nextActiveNodes: NextActiveNodes, latestMajorityHeight: MajorityHeight, metrics: Metrics): F[Unit]
}
