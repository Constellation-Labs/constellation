package org.constellation.p2p

import cats.data.NonEmptyList
import cats.effect.{Blocker, Concurrent, ContextShift, IO, LiftIO, Timer}
import cats.kernel.Order
import cats.syntax.all._
import constellation._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.generic.semiauto._
import io.circe.parser._
import io.circe.{Decoder, Encoder}
import org.constellation._
import org.constellation.domain.cluster.{ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.redownload.RedownloadService.SnapshotProposalsAtHeight
import org.constellation.domain.redownload.{DownloadService, RedownloadStorageAlgebra}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.Cluster.ClusterNode
import org.constellation.rewards.EigenTrust
import org.constellation.schema.snapshot.LatestMajorityHeight
import org.constellation.schema.{Id, NodeState, PeerNotification}
import org.constellation.serialization.KryoSerializer
import org.constellation.session.SessionTokenService
import org.constellation.util.Logging._
import org.constellation.util._

import scala.concurrent.duration._
import scala.util.Random

case class SetNodeStatus(id: Id, nodeStatus: NodeState)

object SetNodeStatus {
  implicit val setNodeStatusDecoder: Decoder[SetNodeStatus] = deriveDecoder
  implicit val setNodeStatusEncoder: Encoder[SetNodeStatus] = deriveEncoder
}

case class SetStateResult(oldState: NodeState, isNewSet: Boolean)
case class PeerData(
  peerMetadata: PeerMetadata,
  majorityHeight: NonEmptyList[MajorityHeight],
  notification: Seq[PeerNotification] = Seq.empty
) {

  def updateJoiningHeight(height: Long): PeerData =
    this.copy(
      majorityHeight = NonEmptyList.one(majorityHeight.head.copy(joined = Some(height))) ++ majorityHeight.tail
    )

  def updateLeavingHeight(leavingHeight: Long): PeerData = {
    val joinedHeight = majorityHeight.head.joined

    val leavingHeightToSet = if (joinedHeight.exists(_ <= leavingHeight)) Some(leavingHeight) else joinedHeight

    this.copy(
      majorityHeight = NonEmptyList.one(majorityHeight.head.copy(left = leavingHeightToSet)) ++ majorityHeight.tail
    )
  }

  def canBeRemoved(lowestMajorityHeight: Long): Boolean =
    majorityHeight.forall(_.left.exists(_ < lowestMajorityHeight))
}

case class PendingRegistration(ip: String, request: PeerRegistrationRequest)

case class Deregistration(ip: String, port: Int, id: Id)

case class JoinedHeight(id: Id, height: Long)

object JoinedHeight {
  implicit val joinedHeightDecoder: Decoder[JoinedHeight] = deriveDecoder
  implicit val joinedHeightEncoder: Encoder[JoinedHeight] = deriveEncoder
}

case object GetPeerInfo

case class UpdatePeerInfo(peerData: PeerData)
case class ChangePeerState(id: Id, state: NodeState)

case class MajorityHeight(joined: Option[Long], left: Option[Long] = None) {
  def isFinite = joined.isDefined && left.isDefined
}

object MajorityHeight {
  implicit val majorityHeightEncoder: Encoder[MajorityHeight] = deriveEncoder
  implicit val majorityHeightDecoder: Decoder[MajorityHeight] = deriveDecoder

  def genesis: MajorityHeight = MajorityHeight(Some(0L), None)

  def isHeightBetween(height: Long)(majorityHeights: NonEmptyList[MajorityHeight]): Boolean =
    majorityHeights.exists(isHeightBetween(height, _))

  def isHeightBetween(height: Long, majorityHeight: MajorityHeight): Boolean =
    majorityHeight.joined.exists(_ < height) && majorityHeight.left.forall(_ >= height)
}

case class UpdatePeerNotifications(notifications: Seq[PeerNotification])

class Cluster[F[_]](
  joiningPeerValidator: JoiningPeerValidator[F],
  apiClient: ClientInterpreter[F],
  sessionTokenService: SessionTokenService[F],
  nodeStorage: NodeStorageAlgebra[F],
  clusterStorage: ClusterStorageAlgebra[F],
  redownloadStorage: RedownloadStorageAlgebra[F],
  downloadService: DownloadService[F],
  eigenTrust: EigenTrust[F],
  processingConfig: ProcessingConfig,
  unboundedBlocker: Blocker,
  nodeId: Id,
  alias: String,
  metrics: Metrics,
  nodeConfig: NodeConfig
)(
  implicit F: Concurrent[F],
  C: ContextShift[F],
  T: Timer[F]
) {

//  private val initialState: NodeState =
//    if (nodeConfig.cliConfig.startOfflineMode) NodeState.Offline else NodeState.PendingDownload
  private val peerDiscovery = PeerDiscovery[F](apiClient, this, nodeId, unboundedBlocker)

  implicit val logger = Slf4jLogger.getLogger[F]

//  metrics.updateMetricAsync[IO]("nodeState", initialState.toString).unsafeRunAsync(_ => ())

  def clusterNodes(): F[List[ClusterNode]] =
    clusterStorage.getPeers.map(_.values.toList.map(_.peerMetadata).map(ClusterNode(_))).flatMap { nodes =>
      nodeStorage.getNodeState
        .map(ClusterNode(alias, nodeId, peerHostPort, _, 0L))
        .map(nodes ++ List(_))
    }

  def deregister(peerUnregister: PeerUnregister): F[Unit] =
    logThread(
      for {
        p <- clusterStorage.getPeers
        _ <- p.get(peerUnregister.id).traverse { peer =>
          updatePeerInfo(
            peer
              .updateLeavingHeight(peerUnregister.majorityHeight)
              .copy(peerMetadata = peer.peerMetadata.copy(nodeState = NodeState.Leaving))
          )
        }
      } yield (),
      "cluster_deregister"
    )

  def markOfflinePeer(nodeId: Id): F[Unit] = logThread(
    {
      implicit val snapOrder: Order[SnapshotProposalsAtHeight] =
        Order.by[SnapshotProposalsAtHeight, List[Long]](a => a.keySet.toList.sorted)

      val leavingFlow: F[Unit] = for {
        notOfflinePeers <- clusterStorage.getNotOfflinePeers
        _ <- logger.info(s"Mark offline peer: ${nodeId}")
        leavingPeer <- notOfflinePeers
          .get(nodeId)
          .fold(
            F.raiseError[PeerData](
              new Throwable(s"Peer id=${nodeId} cannot be find in the peers map or can be already offline")
            )
          )(_.pure[F])
        leavingPeerId = leavingPeer.peerMetadata.id
        majorityHeight <- redownloadStorage.getLatestMajorityHeight

        proposals <- notOfflinePeers.toList.traverse {
          case (_, pd) =>
            timeoutTo(
              PeerResponse
                .run(
                  apiClient.snapshot
                    .getPeerProposals(leavingPeerId),
                  unboundedBlocker
                )(pd.peerMetadata.toPeerClientMetadata)
                .handleErrorWith(
                  _ =>
                    logger.debug(
                      s"Cannot get peer proposals for leaving node (id=${leavingPeerId} host=${leavingPeer.peerMetadata.host}) from peer (id=${pd.peerMetadata.id} host=${pd.peerMetadata.host}). Fallback to empty"
                    ) >> F.pure(None)
                ),
              5.seconds,
              F.pure(none[SnapshotProposalsAtHeight])
            )
        }.flatMap { list =>
          redownloadStorage
            .getPeerProposals(leavingPeerId)
            .map(list.::)
        }.map(_.flatten)

        maxProposal = proposals.maximumOption
        _ <- maxProposal.map { proposal =>
          logger.debug(s"Maximum proposal for leaving node id=${leavingPeerId} is empty? ${proposal.isEmpty}") >>
            redownloadStorage.replacePeerProposals(leavingPeerId, proposal)
        }.getOrElse(F.unit)

        maxProposalHeight = maxProposal.flatMap(_.keySet.toList.maximumOption)
        joiningHeight = leavingPeer.majorityHeight.head.joined

        leavingHeight = maxProposalHeight
          .map(height => if (height > majorityHeight) majorityHeight else height)
          .orElse(joiningHeight)
          .getOrElse(majorityHeight)

        _ <- logger.debug(
          s"Leaving node id=${leavingPeerId} | maxProposalHeight=${maxProposalHeight} | joiningHeight=${joiningHeight} | leavingHeight=${leavingHeight} "
        )

        newPeerData = leavingPeer
          .updateLeavingHeight(leavingHeight)
          .copy(peerMetadata = leavingPeer.peerMetadata.copy(nodeState = NodeState.Offline))

        _ <- clusterStorage.updatePeer(newPeerData)

        _ <- sessionTokenService.clearPeerToken(leavingPeer.peerMetadata.host)

        _ <- logger.info(s"Mark offline peer: ${nodeId} - finished")
      } yield ()

      leavingFlow.handleErrorWith { err =>
        logger.error(s"Error during marking peer as offline: ${err.getMessage}") >> F.raiseError(err)
      }
    },
    "cluster_markOfflinePeer"
  )

  private def timeoutTo[A](fa: F[A], after: FiniteDuration, fallback: F[A]): F[A] = // Consider extracting
    F.race(T.sleep(after), fa).flatMap {
      case Left(_)  => fallback
      case Right(a) => F.pure(a)
    }

  def removePeer(pd: PeerData): F[Unit] =
    logThread(
      redownloadStorage.getLowestMajorityHeight
        .map(height => pd.canBeRemoved(height))
        .ifM(
          for {
            p <- clusterStorage.getPeers
            pm = pd.peerMetadata

            _ <- clusterStorage.removePeer(pm.id)
            // Technically we shouldn't remove it from eigenTrust if we want to keep the trained model for that node
            //        _ <- LiftIO[F].liftIO(dao.eigenTrust.unregisterAgent(pm.id))
          } yield (),
          F.unit
        ),
      "cluster_removePeer"
    )

  def initiateRejoin(): F[Unit] =
    logThread(
      for {
        _ <- if (peersInfoPath.nonEmpty) {
          logger.warn(
            "Found existing peers in persistent storage. Node probably crashed before and will try to rejoin the cluster."
          )
          rejoin()
        } else {
          logger.warn("No peers in persistent storage. Skipping rejoin.")
        }
      } yield (),
      "cluster_initiatePeerReload"
    )

  def rejoin(): F[Unit] =
    //    def attemptRejoin(lastKnownAPIClients: Seq[HostPort]): F[Unit] =
    //      lastKnownAPIClients match {
    //        case Nil => F.raiseError(new RuntimeException("Couldn't rejoin the cluster"))
    //        case hostPort :: remainingHostPorts =>
    //          join(hostPort)
    //            .handleErrorWith(err => {
    //              logger.error(s"Couldn't rejoin via ${hostPort.host}: ${err.getMessage}")
    //              attemptRejoin(remainingHostPorts)
    //            })
    //      }
    for {
      _ <- logger.warn("Trying to rejoin the cluster...")

      lastKnownPeers = parse(peersInfoPath.lines.mkString)
        .map(_.as[Seq[PeerMetadata]])
        .toOption
        .flatMap(_.toOption)
        .getOrElse(Seq.empty[PeerMetadata])
        .map(pm => HostPort(pm.host, pm.httpPort))

      //      _ <- attemptRejoin(lastKnownPeers)
    } yield ()

  def join(hp: PeerClientMetadata): F[Unit] =
    logThread(
      {
        for {
          state <- nodeStorage.getNodeState
          _ <- if (NodeState.canJoin(state)) F.unit else F.raiseError(new Throwable("Node attempted to double join"))
//          _ <- clearServicesBeforeJoin()
          _ <- attemptRegisterPeer(hp)
          _ <- T.sleep(15.seconds)
          _ <- LiftIO[F].liftIO(downloadService.download())
          _ <- broadcastOwnJoinedHeight()
        } yield ()
      }.handleErrorWith { error =>
        logger.error(error)("Error during joining process! Leaving the cluster!") >>
          leave(
            logger.warn("Left the cluster because of failed joining process!")
          )
      },
      "cluster_join"
    )

  def broadcastOwnJoinedHeight(): F[Unit] = {
    val discoverJoinedHeight = for {
      p <- clusterStorage.getNotOfflinePeers
      clients = p.map(_._2.peerMetadata).toList
      maxMajorityHeight <- clients
        .traverse(
          pm =>
            PeerResponse
              .run(
                apiClient.snapshot
                  .getLatestMajorityHeight(),
                unboundedBlocker
              )(pm.toPeerClientMetadata)
              .map(_.some)
              .handleError(_ => none[LatestMajorityHeight])
        )
        .map(_.flatten)
        .map(heights => if (heights.nonEmpty) heights.map(_.highest).max else 0L)
      delay = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

      participatedInGenesisFlow <- nodeStorage.didParticipateInGenesisFlow.map(_.getOrElse(false))
      participatedInRollbackFlow <- nodeStorage.didParticipateInRollbackFlow.map(_.getOrElse(false))
      joinedAsInitialFacilitator <- nodeStorage.didJoinAsInitialFacilitator.map(_.getOrElse(false))

      ownHeight = if (participatedInGenesisFlow && joinedAsInitialFacilitator) 0L
      else if (participatedInRollbackFlow && joinedAsInitialFacilitator) maxMajorityHeight
      else maxMajorityHeight + delay

      _ <- nodeStorage.setOwnJoinedHeight(ownHeight)
    } yield ownHeight

    for {
      height <- nodeStorage.getOwnJoinedHeight
      _ <- logger.debug(s"Broadcasting own joined height - step1: height=$height")
      ownHeight <- height.map(_.pure[F]).getOrElse(discoverJoinedHeight)
      _ <- logger.debug(s"Broadcasting own joined height - step2: height=$ownHeight")
      _ <- broadcast(
        PeerResponse.run(apiClient.cluster.setJoiningHeight(JoinedHeight(nodeId, ownHeight)), unboundedBlocker)
      )
      _ <- F.start(
        T.sleep(10.seconds) >> broadcast(
          PeerResponse.run(apiClient.cluster.setJoiningHeight(JoinedHeight(nodeId, ownHeight)), unboundedBlocker)
        )
      )
      _ <- F.start(
        T.sleep(30.seconds) >> broadcast(
          PeerResponse.run(apiClient.cluster.setJoiningHeight(JoinedHeight(nodeId, ownHeight)), unboundedBlocker)
        )
      )
    } yield ()
  }

  def broadcast[T](
    f: PeerClientMetadata => F[T],
    skip: Set[Id] = Set.empty,
    subset: Set[Id] = Set.empty
  ): F[Map[Id, Either[Throwable, T]]] =
    logThread(
      for {
        peerInfo <- clusterStorage.getNotOfflinePeers
        selected = if (subset.nonEmpty) {
          peerInfo.filterKeys(subset.contains)
        } else {
          peerInfo.filterKeys(id => !skip.contains(id))
        }
        (keys, values) = selected.toList.unzip
        res <- values
          .map(_.peerMetadata.toPeerClientMetadata)
          .map(f)
          .traverse { fa =>
            fa.map(_.asRight[Throwable]).handleErrorWith(_.asLeft[T].pure[F])
          }
          .map(v => keys.zip(v).toMap)
      } yield res,
      "cluster_broadcast"
    )

  // I join to someone
  def attemptRegisterPeer(peerClientMetadata: PeerClientMetadata, isReconciliationJoin: Boolean = false): F[Unit] =
    logThread(
      withMetric(
        {
          {
            if (isReconciliationJoin) F.unit
            else sessionTokenService.createAndSetNewOwnToken().map(_ => ())
          } >>
            nodeConfig.whitelisting
              .contains(peerClientMetadata.id)
              .pure[F]
              .ifM(
                PeerResponse
                  .run(
                    apiClient.sign
                      .getRegistrationRequest(),
                    unboundedBlocker
                  )(peerClientMetadata)
                  .flatMap { registrationRequest =>
                    for {
                      _ <- pendingRegistration(peerClientMetadata.host, registrationRequest, !isReconciliationJoin)
                      prr <- pendingRegistrationRequest(isReconciliationJoin)
                      response <- PeerResponse.run(apiClient.sign.register(prr), unboundedBlocker)(peerClientMetadata)
                    } yield response
                  }
                  .handleErrorWith { err =>
                    logger.error(err)("Registration request failed") >>
                      metrics.incrementMetricAsync[F]("peerGetRegistrationRequestFailed") >>
                      err.raiseError[F, Unit]
                  },
                logger.error(s"unknown peer, not whitelisted")
              )

        },
        "addPeerWithRegistrationSymmetric"
      ),
      "cluster_attemptRegisterPeer"
    )

  def pendingRegistration(
    ip: String,
    request: PeerRegistrationRequest,
    withDiscovery: Boolean = true
  ): F[Unit] =
    logThread(
      for {
        pr <- PendingRegistration(ip, request).pure[F]
        _ <- logger.debug(s"Pending Registration request: $pr")

        validExternalHost = validWithLoopbackGuard(request.host)
        peers <- clusterStorage.getPeers
        // TODO: Move to JoiningPeerValidator
        existsNotOffline = peers.exists {
          case (_, d) =>
            val existingId = d.peerMetadata.id == request.id
            val existingIP = d.peerMetadata.host == request.host
            val canActAsJoiningSource = NodeState
              .canActAsJoiningSource(
                d.peerMetadata.nodeState
              )
            (existingId || existingIP) && canActAsJoiningSource
        }

        // TODO: Move to JoiningPeerValidator
        isCorrectIP = ip == request.host && ip != ""
        // TODO: Move to JoiningPeerValidator
        isWhitelisted = nodeConfig.whitelisting.contains(request.id)
        whitelistingHash = KryoSerializer.serializeAnyRef(nodeConfig.whitelisting).sha256
        validWhitelistingHash = whitelistingHash == request.whitelistingHash

        isSelfId = nodeId == request.id
        badAttempt = !isCorrectIP || !isWhitelisted || !validWhitelistingHash || isSelfId || !validExternalHost || existsNotOffline
        isValid <- joiningPeerValidator.isValid(
          PeerClientMetadata(
            request.host,
            request.port,
            request.id
          )
        )

        _ <- if (!isWhitelisted) {
          metrics.incrementMetricAsync[F]("notWhitelistedAdditionAttempt")
        } else F.unit

        _ <- if (badAttempt) {
          metrics.incrementMetricAsync[F]("badPeerAdditionAttempt")
        } else if (!isValid) {
          metrics.incrementMetricAsync[F]("invalidPeerAdditionAttempt")
        } else {
          val authSignRequest = PeerAuthSignRequest(Random.nextLong())
          val peerClientMetadata = PeerClientMetadata(
            request.host,
            request.port,
            request.id
          )
          val req = PeerResponse.run(apiClient.sign.sign(authSignRequest), unboundedBlocker)(peerClientMetadata)

          req.flatTap { sig =>
            if (sig.hashSignature.id != request.id) {
              logger.warn(
                s"Keys should be the same: ${sig.hashSignature.id} != ${request.id}"
              ) >>
                metrics.incrementMetricAsync[F]("peerKeyMismatch")
            } else F.unit
          }.flatTap { sig =>
            if (!sig.valid(authSignRequest.salt.toString)) {
              logger.warn(s"Invalid peer signature $request $authSignRequest $sig") >>
                metrics.incrementMetricAsync[F]("invalidPeerRegistrationSignature")
            } else F.unit
          }.flatTap { sig =>
            metrics.incrementMetricAsync[F]("peerAddedFromRegistrationFlow") >>
              logger.debug(s"Valid peer signature $request $authSignRequest $sig")
          }.flatMap {
            sig =>
              val updateJoiningHeight = for {
                _ <- nodeStorage.setParticipatedInGenesisFlow(request.participatesInGenesisFlow)
                _ <- nodeStorage.setParticipatedInRollbackFlow(request.participatesInRollbackFlow)
                _ <- nodeStorage.setJoinedAsInitialFacilitator(request.joinsAsInitialFacilitator)
              } yield ()

              val updateToken =
                sessionTokenService.updatePeerToken(ip, request.token)

              for {
                state <- withMetric(
                  PeerResponse.run(apiClient.nodeMetadata.getNodeState(), unboundedBlocker)(peerClientMetadata),
                  "nodeState"
                )(metrics)
                id = sig.hashSignature.id
                existingMajorityHeight <- clusterStorage.getPeers.map(
                  _.get(id).map(_.majorityHeight).filter(_.head.isFinite)
                )
                alias = nodeConfig.whitelisting.get(id).flatten
                peerMetadata = PeerMetadata(
                  request.host,
                  request.port,
                  id,
                  nodeState = state.nodeState,
                  auxAddresses = state.addresses,
                  resourceInfo = request.resourceInfo,
                  alias = alias
                )
                majorityHeight = MajorityHeight(request.majorityHeight)
                majorityHeights = existingMajorityHeight
                  .map(_.prepend(majorityHeight))
                  .getOrElse(NonEmptyList.one(majorityHeight))
                peerData = PeerData(peerMetadata, majorityHeights)
                _ <- updatePeerInfo(peerData) >> updateJoiningHeight >> updateToken >> C.shift >> F.start(
                  if (withDiscovery) peerDiscovery.discoverFrom(peerMetadata) else F.unit
                )
              } yield ()
          }.handleErrorWith { err =>
            logger.warn(
              s"Sign request to ${request.host}:${request.port} failed. $err"
            ) >>
              metrics.incrementMetricAsync[F]("peerSignatureRequestFailed")
          }
        }
      } yield (),
      "cluster_pendingRegistration"
    )

  def updatePeerInfo(peerData: PeerData): F[Unit] =
    for {
      _ <- clusterStorage.registerPeer(peerData)
      _ <- eigenTrust.registerAgent(peerData.peerMetadata.id)
    } yield ()

  private def validWithLoopbackGuard(host: String): Boolean =
    (host != externalHostString && host != "127.0.0.1" && host != "localhost") || !preventLocalhostAsPeer

  // Someone joins to me
  def pendingRegistrationRequest(isReconciliationJoin: Boolean = false): F[PeerRegistrationRequest] = {
    for {
      height <- nodeStorage.getOwnJoinedHeight

      peers <- clusterStorage.getPeers.map(_.filter {
        case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState)
      })
      peersSize = peers.size
      minFacilitatorsSize = processingConfig.numFacilitatorPeers
      isInitialFacilitator = peersSize < minFacilitatorsSize
      participatedInRollbackFlow <- nodeStorage.didParticipateInRollbackFlow.map(_.getOrElse(false))
      participatedInGenesisFlow <- nodeStorage.didParticipateInGenesisFlow.map(_.getOrElse(false))
      whitelistingHash = KryoSerializer.serializeAnyRef(nodeConfig.whitelisting).sha256
      oOwnToken <- sessionTokenService.getOwnToken()
      ownToken <- oOwnToken.map(F.pure).getOrElse(F.raiseError(new Throwable("Own token not set!")))

      _ <- logger.debug(
        s"Pending registration request: ownHeight=$height peers=$peersSize isInitialFacilitator=$isInitialFacilitator participatedInRollbackFlow=$participatedInRollbackFlow participatedInGenesisFlow=$participatedInGenesisFlow"
      )

      prr <- F.delay {
        PeerRegistrationRequest(
          externalHostString,
          externalPeerHTTPPort,
          nodeId,
          ResourceInfo(
            diskUsableBytes = new java.io.File(snapshotPath).getUsableSpace
          ),
          height,
          participatesInGenesisFlow = participatedInGenesisFlow,
          participatesInRollbackFlow = participatedInRollbackFlow,
          joinsAsInitialFacilitator = isInitialFacilitator,
          whitelistingHash,
          ownToken,
          isReconciliationJoin
        )
      }
    } yield prr
  }.handleErrorWith { err =>
    logger.error(err)("Creating pendingRegistrationRequest failed!") >>
      err.raiseError[F, PeerRegistrationRequest]
  }

  def leave(gracefulShutdown: => F[Unit]): F[Unit] =
    logThread(
      for {
        _ <- logger.info("Trying to gracefully leave the cluster")

        majorityHeight <- LiftIO[F].liftIO(redownloadStorage.getLatestMajorityHeight)
        _ <- broadcastLeaveRequest(majorityHeight)
        _ <- compareAndSet(NodeState.all, NodeState.Leaving)

        // TODO: make interval check to wait for last n snapshots, set Offline state only in Leaving see #641 for details
        _ <- Timer[F].sleep(processingConfig.leavingStandbyTimeout.seconds)

        _ <- compareAndSet(NodeState.all, NodeState.Offline)

        _ <- clusterStorage.clearPeers()
        _ <- sessionTokenService.clearOwnToken()
        _ <- if (nodeConfig.isGenesisNode) nodeStorage.setOwnJoinedHeight(0L) else nodeStorage.clearOwnJoinedHeight()
        _ <- eigenTrust.clearAgents()

        _ <- gracefulShutdown
      } yield (),
      "cluster_leave"
    )

  def compareAndSet(expected: Set[NodeState], newState: NodeState, skipBroadcast: Boolean = false): F[SetStateResult] =
    nodeStorage
      .compareAndSet(expected, newState)
      .flatTap(
        res =>
          if (res.isNewSet) LiftIO[F].liftIO(metrics.updateMetricAsync[IO]("nodeState", newState.toString))
          else F.unit
      )
      .flatTap {
        case SetStateResult(_, true) if !skipBroadcast && NodeState.broadcastStates.contains(newState) =>
          broadcastNodeState(newState)
        case _ => F.unit
      }

  private def broadcastNodeState(nodeState: NodeState, nodeId: Id = nodeId): F[Unit] =
    logThread(
      broadcast(
        PeerResponse.run(apiClient.cluster.setNodeStatus(SetNodeStatus(nodeId, nodeState)), unboundedBlocker),
        Set(nodeId)
      ).flatTap {
        _.filter(_._2.isLeft).toList.traverse {
          case (id, e) => logger.warn(s"Unable to propagate status to node ID: $id")
        }
      }.void,
      "cluster_broadcastNodeState"
    )

  private def broadcastLeaveRequest(majorityHeight: Long): F[Unit] = {
    def peerUnregister(c: PeerClientMetadata) =
      PeerUnregister(peerHostPort.host, peerHostPort.port, nodeId, majorityHeight)
    broadcast(c => PeerResponse.run(apiClient.cluster.deregister(peerUnregister(c)), unboundedBlocker)(c)).void
  }

  def broadcastOfflineNodeState(nodeId: Id = nodeId): F[Unit] =
    broadcastNodeState(NodeState.Offline, nodeId)

  private def validPeerAddition(hp: HostPort, peerInfo: Map[Id, PeerData]): Boolean = {
    val hostAlreadyExists = peerInfo.exists {
      case (_, data) => data.peerMetadata.host == hp.host && data.peerMetadata.httpPort == hp.port
    }
    validWithLoopbackGuard(hp.host) && !hostAlreadyExists
  }
}

object Cluster {

  type Peers = Map[Id, PeerData]

  def apply[F[_]: Concurrent: Timer: ContextShift](
    joiningPeerValidator: JoiningPeerValidator[F],
    apiClient: ClientInterpreter[F],
    sessionTokenService: SessionTokenService[F],
    nodeStorage: NodeStorageAlgebra[F],
    clusterStorage: ClusterStorageAlgebra[F],
    redownloadStorage: RedownloadStorageAlgebra[F],
    downloadService: DownloadService[F],
    eigenTrust: EigenTrust[F],
    processingConfig: ProcessingConfig,
    unboundedBlocker: Blocker,
    nodeId: Id,
    alias: String,
    metrics: Metrics,
    nodeConfig: NodeConfig
  ) =
    new Cluster(
      joiningPeerValidator,
      apiClient,
      sessionTokenService,
      nodeStorage,
      clusterStorage,
      redownloadStorage,
      downloadService,
      eigenTrust,
      processingConfig,
      unboundedBlocker,
      nodeId,
      alias,
      metrics,
      nodeConfig
    )

  case class ClusterNode(alias: String, id: Id, ip: HostPort, status: NodeState, reputation: Long)

  object ClusterNode {

    def apply(pm: PeerMetadata) =
      new ClusterNode(pm.alias.getOrElse(pm.id.short), pm.id, HostPort(pm.host, pm.httpPort), pm.nodeState, 0L)

    implicit val clusterNodeEncoder: Encoder[ClusterNode] = deriveEncoder
    implicit val clusterNodeDecoder: Decoder[ClusterNode] = deriveDecoder
  }

}
