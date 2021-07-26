package org.constellation.p2p

import better.files.File
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
import org.constellation.domain.cluster.{BroadcastService, ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.redownload.RedownloadService.SnapshotProposalsAtHeight
import org.constellation.domain.redownload.{DownloadService, RedownloadStorageAlgebra}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.Cluster.ClusterNode
import org.constellation.rewards.EigenTrust
import org.constellation.schema.snapshot.LatestMajorityHeight
import org.constellation.schema.{Id, NodeState, NodeType, PeerNotification}
import org.constellation.serialization.KryoSerializer
import org.constellation.session.SessionTokenService
import org.constellation.util.Logging._
import org.constellation.util._
import org.constellation.collection.MapUtils._
import org.constellation.domain.healthcheck.HealthCheckLoggingHelper.{logId, logIds}
import org.constellation.domain.observation.ObservationService
import org.constellation.schema.observation.{NodeJoinsTheCluster, Observation}

import java.security.KeyPair
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
  observationService: ObservationService[F],
  broadcastService: BroadcastService[F],
  processingConfig: ProcessingConfig,
  unboundedBlocker: Blocker,
  nodeId: Id,
  keyPair: KeyPair,
  alias: String,
  metrics: Metrics,
  nodeConfig: NodeConfig,
  peerHostPort: HostPort,
  peersInfoPath: File,
  externalHostString: String,
  externalPeerHTTPPort: Int,
  snapshotPath: String
)(
  implicit F: Concurrent[F],
  C: ContextShift[F],
  T: Timer[F]
) {

  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")
  val activePeersRotationInterval: Int = ConfigUtil.constellation.getInt("snapshot.activePeersRotationInterval")
  val activePeersRotationEveryNHeights: Int = snapshotHeightInterval * activePeersRotationInterval

  private val peerDiscovery = PeerDiscovery[F](apiClient, this, clusterStorage, nodeId, unboundedBlocker)

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
        maxProposal <- redownloadStorage
          .getPeerProposals(leavingPeerId)
          .map(_.flatMap(_.maxHeightEntry))

        maxProposalHeight = maxProposal.map(_._1)
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
          joiningHeight <- discoverJoiningHeight()
          // TODO: verify if this flow is still correct after refactor given joining pool functionality
          _ <- clusterStorage.isAnActiveFullPeer.ifM(
            downloadService.download(joiningHeight),
            notifyCurrentActiveFullNodesAboutJoining() >>
              compareAndSet(NodeState.initial, NodeState.Ready).void
          )
          _ <- setOwnJoinedHeight()
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

  def discoverJoiningHeight(): F[Long] =
    for {
      p <- clusterStorage.getJoinedPeers
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
    } yield ownHeight

  def setOwnJoinedHeight(): F[Unit] =
    for {
      height <- nodeStorage.getOwnJoinedHeight
      _ <- height
        .map(_.pure[F])
        .getOrElse(discoverJoiningHeight().flatTap {
          nodeStorage.setOwnJoinedHeight
        }.flatTap {
          metrics.updateMetricAsync[F]("cluster_ownJoinedHeight", _)
        })
    } yield ()

  def broadcastOwnJoinedHeight(): F[Unit] =
    for {
      height <- nodeStorage.getOwnJoinedHeight
      ownHeight <- if (height.isEmpty) F.raiseError(new Throwable("Own joined height not set!")) else height.get.pure[F]
      _ <- logger.debug(s"Broadcasting own joined height - step2: height=$ownHeight")
      _ <- broadcastService.broadcast(
        PeerResponse.run(apiClient.cluster.setJoiningHeight(JoinedHeight(nodeId, ownHeight)), unboundedBlocker),
        forceAllPeers = true
      )
      _ <- F.start(
        T.sleep(10.seconds) >> broadcastService.broadcast(
          PeerResponse.run(apiClient.cluster.setJoiningHeight(JoinedHeight(nodeId, ownHeight)), unboundedBlocker),
          forceAllPeers = true
        )
      )
      _ <- F.start(
        T.sleep(30.seconds) >> broadcastService.broadcast(
          PeerResponse.run(apiClient.cluster.setJoiningHeight(JoinedHeight(nodeId, ownHeight)), unboundedBlocker),
          forceAllPeers = true
        )
      )
    } yield ()

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
      )(metrics),
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
                  alias = alias,
                  nodeType = request.nodeType
                )
                majorityHeight = MajorityHeight(request.majorityHeight)
                majorityHeights = existingMajorityHeight
                  .map(_.prepend(majorityHeight))
                  .getOrElse(NonEmptyList.one(majorityHeight))
                peerData = PeerData(peerMetadata, majorityHeights)
                _ <- updatePeerInfo(peerData) /*>> updateJoiningHeight*/ >> updateToken >> C.shift >> F.start(
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
    (host != externalHostString && host != "127.0.0.1" && host != "localhost")

  // Someone joins to me
  def pendingRegistrationRequest(isReconciliationJoin: Boolean = false): F[PeerRegistrationRequest] = {
    for {
      height <- nodeStorage.getOwnJoinedHeight

      peers <- clusterStorage.getNotOfflinePeers
      peersSize = peers.size
      minFacilitatorsSize = processingConfig.numFacilitatorPeers
      isInitialFacilitator = peersSize < minFacilitatorsSize
      participatedInRollbackFlow <- nodeStorage.didParticipateInRollbackFlow.map(_.getOrElse(false))
      participatedInGenesisFlow <- nodeStorage.didParticipateInGenesisFlow.map(_.getOrElse(false))
      whitelistingHash = KryoSerializer.serializeAnyRef(nodeConfig.whitelisting).sha256
      oOwnToken <- sessionTokenService.getOwnToken()
      ownToken <- oOwnToken.map(F.pure).getOrElse(F.raiseError(new Throwable("Own token not set!")))
      nodeType = nodeConfig.nodeType

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
          nodeType,
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

        majorityHeight <- redownloadStorage.getLatestMajorityHeight
        _ <- broadcastLeaveRequest(majorityHeight)
        _ <- compareAndSet(NodeState.all, NodeState.Leaving)

        // TODO: make interval check to wait for last n snapshots, set Offline state only in Leaving see #641 for details
        _ <- Timer[F].sleep(processingConfig.leavingStandbyTimeout.seconds)

        _ <- compareAndSet(NodeState.all, NodeState.Offline)

        _ <- clusterStorage.clearPeers()
        _ <- sessionTokenService.clearOwnToken()
        _ <- if (nodeConfig.isGenesisNode)
          nodeStorage.setOwnJoinedHeight(0L) >> metrics.updateMetricAsync[F]("cluster_ownJoinedHeight", 0L)
        else nodeStorage.clearOwnJoinedHeight()
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
          if (res.isNewSet) metrics.updateMetricAsync[F]("nodeState", newState.toString)
          else F.unit
      )
      .flatTap {
        case SetStateResult(_, true) if !skipBroadcast && NodeState.broadcastStates.contains(newState) =>
          broadcastNodeState(newState)
        case _ => F.unit
      }

  private def broadcastNodeState(nodeState: NodeState, nodeId: Id = nodeId): F[Unit] =
    logThread(
      broadcastService
        .broadcast(
          PeerResponse.run(apiClient.cluster.setNodeStatus(SetNodeStatus(nodeId, nodeState)), unboundedBlocker),
          Set(nodeId),
          forceAllPeers = true
        )
        .flatTap {
          _.filter(_._2.isLeft).toList.traverse {
            case (id, e) => logger.warn(s"Unable to propagate status to node ID: $id")
          }
        }
        .void,
      "cluster_broadcastNodeState"
    )

  private def broadcastLeaveRequest(majorityHeight: Long): F[Unit] = {
    def peerUnregister(c: PeerClientMetadata) =
      PeerUnregister(peerHostPort.host, peerHostPort.port, nodeId, majorityHeight)

    broadcastService
      .broadcast(
        c => PeerResponse.run(apiClient.cluster.deregister(peerUnregister(c)), unboundedBlocker)(c),
        forceAllPeers = true
      )
      .void
  }

  def broadcastOfflineNodeState(nodeId: Id = nodeId): F[Unit] =
    broadcastNodeState(NodeState.Offline, nodeId)

  private def validPeerAddition(hp: HostPort, peerInfo: Map[Id, PeerData]): Boolean = {
    val hostAlreadyExists = peerInfo.exists {
      case (_, data) => data.peerMetadata.host == hp.host && data.peerMetadata.httpPort == hp.port
    }
    validWithLoopbackGuard(hp.host) && !hostAlreadyExists
  }

  private def notifyCurrentActiveFullNodesAboutJoining(): F[Unit] = {
    // TODO: is using tailRecM like this tail recursive already? :D
    def loopNotifyUntilSuccess(lastActivePeersData: List[PeerData]): F[Unit] =
      F.tailRecM(lastActivePeersData) {
        case Nil =>
          F.raiseError(new Throwable("Run out of active Full nodes when attempted to notify about cluster joining!"))
        case peer :: otherPeers =>
          logger.debug(s"Trying to send joining pool observation to ${peer.peerMetadata.id.medium}") >>
            PeerResponse
              .run(apiClient.cluster.notifyAboutClusterJoin(), unboundedBlocker)(peer.peerMetadata.toPeerClientMetadata)
              .flatMap(_ => logger.debug(s"Success sending joining pool observation to ${peer.peerMetadata.id.medium}"))
              .map(_.asRight[List[PeerData]])
              .handleErrorWith { e =>
                logger.debug(e)(
                  s"Error sending join notification to active peer ${logId(peer.peerMetadata.id)}. Trying next peers: ${logIds(otherPeers.map(_.peerMetadata.id).toSet)}"
                ) >>
                  otherPeers.asLeft[Unit].pure[F]
              }
      }

    for {
      fullNodes <- clusterStorage.getFullPeers()
      _ <- logger.debug(
        s"Full nodes to broadcast request about current active full nodes to. Nodes ${fullNodes.keySet.map(_.medium)}"
      )
      lastActivePeers <- broadcastService
        .broadcast(
          PeerResponse.run(apiClient.cluster.getActiveFullNodes(), unboundedBlocker),
          subset = fullNodes.keySet
        )
        .map(_.values.toList.separate._2.flatten.toSet)
        .flatMap {
          case setsOfActive if setsOfActive.size == 1 =>
            logger.debug(s"Unanimous full active nodes indication! ${setsOfActive.flatten.map(_.medium)}") >>
              setsOfActive.flatten.pure[F]
          case setsOfActive =>
            logger.debug(s"Non-unanimous full active nodes indication! ${setsOfActive.map(_.map(_.medium))}") >>
              Set.empty[Id].pure[F]
        }

      lastActivePeersData <- lastActivePeers.toList
        .traverse(clusterStorage.getPeer)
        .map(_.flatten)
      _ <- loopNotifyUntilSuccess(lastActivePeersData)
    } yield ()
  }

  def handleJoiningClusterNotification(id: Id): F[Unit] = // TODO: pass observation service explicitly to Cluster
    observationService
      .put(
        Observation.create(id, NodeJoinsTheCluster)(keyPair)
      )
      .flatMap(obs => logger.debug(s"Registered NodeJoinedTheCluster observation: $obs")) >>
      metrics.incrementMetricAsync("processedNodeJoinedTheClusterObservation")
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
    observationService: ObservationService[F],
    broadcastService: BroadcastService[F],
    processingConfig: ProcessingConfig,
    unboundedBlocker: Blocker,
    nodeId: Id,
    keyPair: KeyPair,
    alias: String,
    metrics: Metrics,
    nodeConfig: NodeConfig,
    peerHostPort: HostPort,
    peersInfoPath: File,
    externalHostString: String,
    externalPeerHTTPPort: Int,
    snapshotPath: String
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
      observationService,
      broadcastService,
      processingConfig,
      unboundedBlocker,
      nodeId,
      keyPair,
      alias,
      metrics,
      nodeConfig,
      peerHostPort,
      peersInfoPath,
      externalHostString,
      externalPeerHTTPPort,
      snapshotPath
    )

  case class ClusterNode(alias: String, id: Id, ip: HostPort, status: NodeState, reputation: Long)

  object ClusterNode {

    def apply(pm: PeerMetadata) =
      new ClusterNode(pm.alias.getOrElse(pm.id.short), pm.id, HostPort(pm.host, pm.httpPort), pm.nodeState, 0L)

    implicit val clusterNodeEncoder: Encoder[ClusterNode] = deriveEncoder
    implicit val clusterNodeDecoder: Decoder[ClusterNode] = deriveDecoder
  }

  case class MissingActivePeers(ids: Set[Id], nodeType: NodeType)
      extends Throwable(s"Missing active $nodeType peers: $ids")

  def calculateActiveBetweenHeights(starting: Long, rotationInterval: Long): MajorityHeight = {
    val ending = starting + rotationInterval
    // TODO: for sure it can be done better
    MajorityHeight(if (starting < 2L) starting.some else (starting - 2L).some, ending.some)
  }

}
