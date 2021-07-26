package org.constellation.infrastructure.cluster

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Clock, Sync}
import cats.syntax.all._
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.p2p.Cluster.MissingActivePeers
import org.constellation.p2p.{JoinedHeight, MajorityHeight, PeerData}
import org.constellation.schema.NodeType.{Full, Light}
import org.constellation.schema.snapshot.NextActiveNodes
import org.constellation.schema.{Id, NodeState, NodeType}
import org.constellation.util.Metrics

class ClusterStorageInterpreter[F[_]](nodeId: Id, nodeType: NodeType)(implicit F: Sync[F], C: Clock[F])
    extends ClusterStorageAlgebra[F] {

  private val peers: Ref[F, Map[Id, PeerData]] = Ref.unsafe(Map.empty[Id, PeerData])

  // TODO: move initialization of the refs below to somewhere outside - Cluster class startup?
  private val authorizedPeers: Ref[F, Set[Id]] = Ref.unsafe[F, Set[Id]](Set.empty)
  private val activeFullNodes: Ref[F, Set[Id]] = Ref.unsafe[F, Set[Id]](Set.empty)
  private val activeLightNodes: Ref[F, Set[Id]] = Ref.unsafe[F, Set[Id]](Set.empty)
  private val isActiveFullPeer: Ref[F, Boolean] = Ref.unsafe[F, Boolean](false)
  private val isActiveLightPeer: Ref[F, Boolean] = Ref.unsafe[F, Boolean](false)
  private val activeBetweenHeights: Ref[F, Option[MajorityHeight]] = Ref.unsafe[F, Option[MajorityHeight]](None)

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

  def getReadyPeers: F[Map[Id, PeerData]] =
    getPeers.map(_.filter(eqNodeState(NodeState.readyStates)))

  def getJoinedPeers: F[Map[Id, PeerData]] =
    getNotOfflinePeers.map(_.filter(_._2.majorityHeight.head.joined.nonEmpty))

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
      _.filter(p => NodeState.isNotOffline(p._2.peerMetadata.nodeState))
    }

  def clearPeers(): F[Unit] =
    peers.modify(_ => (Map.empty, ()))

  def getActiveBetweenHeights: F[MajorityHeight] =
    activeBetweenHeights.get.map(_.getOrElse(MajorityHeight(None, None)))

  def clearActiveBetweenHeights(): F[Unit] =
    activeBetweenHeights.modify(_ => (None, ()))

  def getAuthorizedPeers: F[Set[Id]] = authorizedPeers.get

  def setAuthorizedPeers(peers: Set[Id]): F[Unit] =
    authorizedPeers.modify(_ => (peers, ()))

  def addAuthorizedPeer(id: Id): F[Unit] =
    authorizedPeers.modify(current => (current + id, ()))

  def removeAuthorizedPeer(id: Id): F[Unit] =
    authorizedPeers.modify(current => (current - id, ()))

  def isAnActiveFullPeer: F[Boolean] = isActiveFullPeer.get

  def isAnActiveLightPeer: F[Boolean] = isActiveLightPeer.get

  def isAnActivePeer: F[Boolean] =
    for {
      isActiveFullPeer <- isAnActiveFullPeer
      isActiveLightPeer <- isAnActiveLightPeer
    } yield isActiveLightPeer || isActiveFullPeer

  def getFullPeers(): F[Map[Id, PeerData]] =
    getPeers.map(_.filter { case (_, peerData) => peerData.peerMetadata.nodeType == Full })

  def getFullPeersIds(withSelfId: Boolean): F[Set[Id]] =
    getFullPeers().map(_.keySet ++ (if (nodeType == Full) Set(nodeId) else Set.empty))

  def getLightPeers(): F[Map[Id, PeerData]] =
    getPeers.map(_.filter { case (_, peerData) => peerData.peerMetadata.nodeType == Light })

  def getLightPeersIds(withSelfId: Boolean): F[Set[Id]] =
    getLightPeers().map(_.keySet ++ (if (nodeType == Light) Set(nodeId) else Set.empty))

  def getActiveFullPeersIds(withSelfId: Boolean): F[Set[Id]] =
    for {
      activeFullNodes <- activeFullNodes.get
      ownId <- isAnActiveFullPeer
        .map(_ && withSelfId)
        .ifM(
          Set(nodeId).pure[F],
          Set.empty[Id].pure[F]
        )
    } yield activeFullNodes ++ ownId

  def getActiveFullPeers(): F[Map[Id, PeerData]] =
    for {
      allPeers <- getPeers
      activeFullNodes <- getActiveFullPeersIds()
      activePeersMaybeData = activeFullNodes
        .map(id => id -> allPeers.get(id))
        .toMap
      activeBetweenHeights <- getActiveBetweenHeights
      activePeersData <- activePeersMaybeData match {
        case peers if peers.forall(_._2.nonEmpty) =>
          peers.mapValues(_.get.copy(majorityHeight = NonEmptyList.one(activeBetweenHeights))).pure[F]
        case incompleteActivePeers =>
          val notFound = incompleteActivePeers.collect { case (id, None) => id }.toSet
          F.raiseError(MissingActivePeers(notFound, Full))
      }
    } yield activePeersData

  def setActiveFullPeers(peers: Set[Id]): F[Unit] =
    activeFullNodes.modify(_ => (peers, ()))

  def getActiveLightPeersIds(withSelfId: Boolean = false): F[Set[Id]] =
    for {
      activeLightNodes <- activeLightNodes.get
      ownId <- isAnActiveLightPeer
        .map(_ && withSelfId)
        .ifM(
          Set(nodeId).pure[F],
          Set.empty[Id].pure[F]
        )
    } yield activeLightNodes ++ ownId

  def getActiveLightPeers(): F[Map[Id, PeerData]] =
    for {
      allPeers <- getPeers
      activeLightNodes <- getActiveLightPeersIds()
      activePeersData = activeLightNodes
        .map(id => id -> allPeers.get(id))
        .collect { case (id, Some(peerData)) => id -> peerData }
        .toMap
      notFound = activeLightNodes -- activePeersData.keySet
      peersData <- if (notFound.isEmpty)
        activePeersData.pure[F]
      else {
        F.raiseError(MissingActivePeers(notFound, Light))
      }
    } yield peersData

  def getActivePeersIds(withSelfId: Boolean): F[Set[Id]] =
    for {
      activeLight <- getActiveLightPeersIds(withSelfId)
      activeFull <- getActiveFullPeersIds(withSelfId)
    } yield activeLight ++ activeFull

  def getActivePeers: F[Map[Id, PeerData]] =
    for {
      activeLight <- getActiveLightPeers()
      activeFull <- getActiveFullPeers()
    } yield activeLight ++ activeFull

  def setAsActivePeer(asType: NodeType): F[Unit] =
    (nodeType, asType) match {
      case (Full, Full) =>
        isActiveFullPeer.modify(_ => (true, ()))
      case (Light, Light) =>
        isActiveLightPeer.modify(_ => (true, ()))
      case (_, _) =>
        F.raiseError(new Throwable("Asked to join the network as a different node type!"))
    }

  def unsetAsActivePeer(): F[Unit] =
    isActiveFullPeer.modify(_ => (false, ())) >>
      isActiveLightPeer.modify(_ => (false, ()))

  def setActiveBetweenHeights(majorityHeight: MajorityHeight): F[Unit] =
    activeBetweenHeights.modify(_ => (majorityHeight.some, ()))
  // TODO: move logic below outside clusterStorage
//    +  def setActiveBetweenHeights(starting: Long): F[Unit] =
//    +    activeBetweenHeights.modify { _ =>
//      +      val ending = starting + activePeersRotationEveryNHeights
//      +      // TODO: for sure it can be done better
//        +      (MajorityHeight(if (starting < 2L ) starting.some else (starting - 2L).some, ending.some).some, ())
//      +    }

  def setActivePeers(
    nextActiveNodes: NextActiveNodes,
    latestMajorityHeight: MajorityHeight,
    metrics: Metrics
  ): F[Unit] =
    if (nextActiveNodes.full.contains(nodeId))
      activeFullNodes.modify(_ => (nextActiveNodes.full - nodeId, ())) >>
        activeLightNodes.modify(_ => (nextActiveNodes.light, ())) >>
        setActiveBetweenHeights(latestMajorityHeight) >>
        setAsActivePeer(Full) >>
        metrics.updateMetricAsync("snapshot_isMemberOfFullActivePool", 1) >>
        metrics.updateMetricAsync("snapshot_isMemberOfLightActivePool", 0)
    else if (nextActiveNodes.light.contains(nodeId))
      activeLightNodes.modify(_ => (nextActiveNodes.light - nodeId, ())) >>
        activeFullNodes.modify(_ => (nextActiveNodes.full, ())) >>
        setAsActivePeer(Light) >>
        metrics.updateMetricAsync("snapshot_isMemberOfFullActivePool", 0) >>
        metrics.updateMetricAsync("snapshot_isMemberOfLightActivePool", 1)
    else
      activeFullNodes.modify(_ => (Set.empty, ())) >>
        activeLightNodes.modify(_ => (Set.empty, ())) >>
        unsetAsActivePeer() >>
        clearActiveBetweenHeights() >>
        metrics.updateMetricAsync("snapshot_isMemberOfFullActivePool", 0) >>
        metrics.updateMetricAsync("snapshot_isMemberOfLightActivePool", 0)

}
