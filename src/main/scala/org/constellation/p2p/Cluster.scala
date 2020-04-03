package org.constellation.p2p

import java.time.LocalDateTime

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Timer}
import cats.implicits._
import constellation._
import enumeratum._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Decoder, Encoder}
import org.constellation._
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.Cluster.ClusterNode
import org.constellation.primitives.IPManager
import org.constellation.primitives.Schema.NodeState
import org.constellation.primitives.Schema.NodeState.broadcastStates
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.util.Logging._
import org.constellation.util._

import scala.concurrent.duration._
import scala.util.Random
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.parser._

case class SetNodeStatus(id: Id, nodeStatus: NodeState)
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

case object GetPeerInfo

case class UpdatePeerInfo(peerData: PeerData)
case class ChangePeerState(id: Id, state: NodeState)

case class PeerNotification(id: Id, state: PeerState, timestamp: LocalDateTime = LocalDateTime.now()) extends Signable

case class MajorityHeight(joined: Option[Long], left: Option[Long] = None) {
  def isFinite = joined.isDefined && left.isDefined
}

object MajorityHeight {
  def genesis: MajorityHeight = MajorityHeight(Some(0L), None)

  def isHeightBetween(height: Long, majorityHeight: MajorityHeight): Boolean =
    majorityHeight.joined.exists(_ < height) && majorityHeight.left.forall(_ >= height)

  def isHeightBetween(height: Long)(majorityHeights: NonEmptyList[MajorityHeight]): Boolean =
    majorityHeights.exists(isHeightBetween(height, _))
}

sealed trait PeerState extends EnumEntry

object PeerState extends Enum[PeerState] with CirceEnum[PeerState] {
  case object Join extends PeerState
  case object Leave extends PeerState

  val values = findValues
}
case class UpdatePeerNotifications(notifications: Seq[PeerNotification])

class Cluster[F[_]](
  ipManager: IPManager[F],
  joiningPeerValidator: JoiningPeerValidator[F],
  apiClient: ClientInterpreter[F],
  dao: DAO
)(
  implicit F: Concurrent[F],
  C: ContextShift[F],
  T: Timer[F]
) {

  def id = dao.id

  private val ownJoinedHeight: Ref[F, Option[Long]] = Ref.unsafe[F, Option[Long]](None)
  private val participatedInGenesisFlow: Ref[F, Option[Boolean]] = Ref.unsafe[F, Option[Boolean]](None)
  private val participatedInRollbackFlow: Ref[F, Option[Boolean]] = Ref.unsafe[F, Option[Boolean]](None)
  private val joinedAsInitialFacilitator: Ref[F, Option[Boolean]] = Ref.unsafe[F, Option[Boolean]](None)

  private val initialState: NodeState =
    if (dao.nodeConfig.cliConfig.startOfflineMode) NodeState.Offline else NodeState.PendingDownload
  private val nodeState: Ref[F, NodeState] = Ref.unsafe[F, NodeState](initialState)
  private val peers: Ref[F, Map[Id, PeerData]] = Ref.unsafe[F, Map[Id, PeerData]](Map.empty)

  implicit val logger = Slf4jLogger.getLogger[F]

  implicit val shadedDao: DAO = dao

  dao.metrics.updateMetricAsync[IO]("nodeState", initialState.toString).unsafeRunAsync(_ => ())
  private val stakingAmount = ConfigUtil.getOrElse("constellation.staking-amount", 0L)

  def setJoinedAsInitialFacilitator(joined: Boolean): F[Unit] =
    joinedAsInitialFacilitator.modify { j =>
      (Some(j.getOrElse(joined)), ())
    }

  def setParticipatedInGenesisFlow(participated: Boolean): F[Unit] =
    participatedInGenesisFlow.modify { p =>
      (Some(p.getOrElse(participated)), ())
    }

  def setParticipatedInRollbackFlow(participated: Boolean): F[Unit] =
    participatedInRollbackFlow.modify { p =>
      (Some(p.getOrElse(participated)), ())
    }

  def setOwnJoinedHeight(height: Long): F[Unit] =
    ownJoinedHeight.modify { h =>
      val updated = Some(h.getOrElse(height))
      (updated, updated.get)
    }.flatMap(dao.metrics.updateMetricAsync("cluster_ownJoinedHeight", _))

  def getOwnJoinedHeight(): F[Option[Long]] =
    ownJoinedHeight.get

  def clearOwnJoinedHeight(): F[Unit] =
    ownJoinedHeight.modify { _ =>
      (None, ())
    }

  // TODO: wkoszycki consider using complex structure of peers so the lookup has O(n) complexity
  def getPeerData(host: String): F[Option[PeerData]] =
    peers.get
      .map(m => m.find(t => t._2.peerMetadata.host == host).map(_._2))

  def clusterNodes(): F[List[ClusterNode]] =
    getPeerInfo.map(_.values.toList.map(_.peerMetadata).map(ClusterNode(_))).flatMap { nodes =>
      getNodeState.map(ClusterNode(dao.id, dao.peerHostPort, _, 0L)).map(nodes ++ List(_))
    }

  def getPeerInfo: F[Map[Id, PeerData]] = peers.get

  def getNodeState: F[NodeState] = nodeState.get

  def updatePeerNotifications(notifications: List[PeerNotification]): F[Unit] =
    logThread(
      for {
        p <- peers.get
        peerUpdate = notifications.flatMap { n =>
          p.get(n.id).map { p =>
            p.copy(notification = p.notification.diff(Seq(n)))
          }
        }
        _ <- logger.debug(s"Peer update: ${peerUpdate}")
        _ <- peerUpdate.traverse(updatePeerInfo) // TODO: bulk update
      } yield (),
      "cluster_updatePeerNotifications"
    )

  def updateJoinedHeight(joinedHeight: JoinedHeight): F[Unit] = {
    val peerId = joinedHeight.id

    peers.modify { m =>
      m.get(peerId) match {
        case Some(peerData) =>
          (
            m.updated(
              peerId,
              peerData.updateJoiningHeight(joinedHeight.height)
            ),
            ()
          )
        case _ => (m, ())
      }
    }
  }

  def setNodeStatus(id: Id, state: NodeState): F[Unit] =
    logThread(
      for {
        _ <- peers.modify(
          p =>
            (
              p.get(id)
                .map { pd =>
                  p + (id -> pd.copy(peerMetadata = pd.peerMetadata.copy(nodeState = state)))
                }
                .getOrElse(p),
              p
            )
        )

        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_setNodeStatus"
    )

  def peerDiscovery(peerMetadata: PeerMetadata): F[Unit] =
    for {
      peersMetadata <- apiClient.nodeMetadata.getPeers().run(peerMetadata.toPeerClientMetadata).handleErrorWith { err =>
        dao.metrics.incrementMetricAsync[F]("peerDiscoveryQueryFailed") >> err.raiseError[F, Seq[PeerMetadata]]
      }
      peers <- getPeerInfo
      filteredPeers = peersMetadata.filter { p =>
        p.id != dao.id && validPeerAddition(HostPort(p.host, p.httpPort), peers)
      }
      register <- filteredPeers.toList.traverse { md =>
        apiClient.sign.getRegistrationRequest().run(md.toPeerClientMetadata).map((md, _))
      }

      _ <- register.traverse(r => pendingRegistration(r._1.host, r._2))
      registerResponse <- register.traverse { md =>
        for {
          prr <- pendingRegistrationRequest
          _ <- apiClient.sign.register(prr).run(md._1.toPeerClientMetadata)
        } yield ()
      }.void
    } yield registerResponse

  def pendingRegistration(ip: String, request: PeerRegistrationRequest): F[Unit] =
    logThread(
      for {
        pr <- PendingRegistration(ip, request).pure[F]
        _ <- logger.debug(s"Pending Registration request: $pr")

        validExternalHost = validWithLoopbackGuard(request.host)
        peers <- peers.get
        existsNotOffline = peers.exists {
          case (_, d) =>
            d.peerMetadata.host == request.host && d.peerMetadata.httpPort == request.port && NodeState
              .canActAsJoiningSource(
                d.peerMetadata.nodeState
              )
        }

        isCorrectIP = ip == request.host && ip != ""
        isWhitelisted = dao.nodeConfig.whitelisting.get(ip).contains(request.id)
        whitelistingHash = KryoSerializer.serializeAnyRef(dao.nodeConfig.whitelisting).sha256
        validWhitelistingHash = whitelistingHash == request.whitelistingHash

        isSelfId = dao.id == request.id
        badAttempt = !isCorrectIP || !isWhitelisted || !validWhitelistingHash || isSelfId || !validExternalHost || existsNotOffline
        isValid <- joiningPeerValidator.isValid(PeerClientMetadata(request.host, request.port, request.id))

        _ <- if (!isWhitelisted) {
          dao.metrics.incrementMetricAsync[F]("notWhitelistedAdditionAttempt")
        } else F.unit

        _ <- if (badAttempt) {
          dao.metrics.incrementMetricAsync[F]("badPeerAdditionAttempt")
        } else if (!isValid) {
          dao.metrics.incrementMetricAsync[F]("invalidPeerAdditionAttempt")
        } else {
          val authSignRequest = PeerAuthSignRequest(Random.nextLong())
          val peerClientMetadata = PeerClientMetadata(request.host, request.port, request.id)
          val req = apiClient.sign.sign(authSignRequest).run(peerClientMetadata)

          req.flatTap { sig =>
            if (sig.hashSignature.id != request.id) {
              logger.warn(s"Keys should be the same: ${sig.hashSignature.id} != ${request.id}") >>
                dao.metrics.incrementMetricAsync[F]("peerKeyMismatch")
            } else F.unit
          }.flatTap { sig =>
            if (!sig.valid(authSignRequest.salt.toString)) {
              logger.warn(s"Invalid peer signature $request $authSignRequest $sig") >>
                dao.metrics.incrementMetricAsync[F]("invalidPeerRegistrationSignature")
            } else F.unit
          }.flatTap { sig =>
            dao.metrics.incrementMetricAsync[F]("peerAddedFromRegistrationFlow") >>
              logger.debug(s"Valid peer signature $request $authSignRequest $sig")
          }.flatMap {
            sig =>
              val updateJoiningHeight = for {
                _ <- setParticipatedInGenesisFlow(request.participatesInGenesisFlow)
                _ <- setParticipatedInRollbackFlow(request.participatesInRollbackFlow)
                _ <- setJoinedAsInitialFacilitator(request.joinsAsInitialFacilitator)
              } yield ()

              for {
                state <- withMetric(
                  apiClient.nodeMetadata.getNodeState().run(peerClientMetadata),
                  "nodeState"
                )
                id = sig.hashSignature.id
                existingMajorityHeight <- getPeerInfo.map(
                  _.get(id).map(_.majorityHeight).filter(_.head.isFinite)
                )
                peerMetadata = PeerMetadata(
                  request.host,
                  request.port,
                  id,
                  nodeState = state.nodeState,
                  auxAddresses = state.addresses,
                  resourceInfo = request.resourceInfo
                )
                majorityHeight = MajorityHeight(request.majorityHeight)
                majorityHeights = existingMajorityHeight
                  .map(_.prepend(majorityHeight))
                  .getOrElse(NonEmptyList.one(majorityHeight))
                peerData = PeerData(peerMetadata, majorityHeights)
                _ <- updatePeerInfo(peerData) >> updateJoiningHeight >> C.shift >> F.start(peerDiscovery(peerMetadata))
              } yield ()
          }.handleErrorWith { err =>
            logger.warn(s"Sign request to ${request.host}:${request.port} failed. $err") >>
              dao.metrics.incrementMetricAsync[F]("peerSignatureRequestFailed")
          }
        }
      } yield (),
      "cluster_pendingRegistration"
    )

  def broadcastOwnJoinedHeight(): F[Unit] = {
    val discoverJoinedHeight = for {
      p <- peers.get.map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
      clients = p.map(_._2.peerMetadata).toList
      maxMajorityHeight <- clients
        .traverse(pm => apiClient.snapshot.getLatestMajorityHeight().run(pm.toPeerClientMetadata))
        .map(heights => if (heights.nonEmpty) heights.map(_.highest).max else 0L)
      delay = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

      participatedInGenesisFlow <- participatedInGenesisFlow.get.map(_.getOrElse(false))
      participatedInRollbackFlow <- participatedInRollbackFlow.get.map(_.getOrElse(false))
      joinedAsInitialFacilitator <- joinedAsInitialFacilitator.get.map(_.getOrElse(false))

      ownHeight = if (participatedInGenesisFlow && joinedAsInitialFacilitator) 0L
      else if (participatedInRollbackFlow && joinedAsInitialFacilitator) maxMajorityHeight
      else maxMajorityHeight + delay

      _ <- setOwnJoinedHeight(ownHeight)
    } yield ownHeight

    for {
      height <- getOwnJoinedHeight()
      _ <- logger.debug(s"Broadcasting own joined height - step1: height=$height")
      ownHeight <- height.map(_.pure[F]).getOrElse(discoverJoinedHeight)
      _ <- logger.debug(s"Broadcasting own joined height - step2: height=$ownHeight")
      _ <- broadcast(apiClient.cluster.setJoiningHeight(JoinedHeight(dao.id, ownHeight)).run)
    } yield ()
  }

  def deregister(peerUnregister: PeerUnregister): F[Unit] =
    logThread(
      for {
        p <- peers.get
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

  def updatePeerInfo(peerData: PeerData): F[Unit] =
    logThread(
      for {
        _ <- peers.modify(p => (p + (peerData.peerMetadata.id -> peerData), p))
        ip = peerData.peerMetadata.host
        _ <- ipManager.addKnownIP(ip)
        _ <- logger.debug(s"Added $ip to known peers.")
        _ <- LiftIO[F].liftIO(dao.registerAgent(peerData.peerMetadata.id))
        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_updatePeerInfo"
    )

  private def updateMetrics(): F[Unit] =
    peers.get.flatMap { p =>
      dao.metrics.updateMetricAsync[F](
        "peers",
        p.map {
          case (idI, clientI) =>
            val addr = s"http://${clientI.peerMetadata.host}:${clientI.peerMetadata.httpPort - 1}"
            s"${idI.short} API: $addr"
        }.mkString(" --- ")
      )
    }

  private def updatePersistentStore(): F[Unit] =
    peers.get.flatMap { p =>
      F.delay(dao.peersInfoPath.write(p.values.toSeq.map(_.peerMetadata).asJson.noSpaces))
    }

  def markOfflinePeer(nodeId: Id): F[Unit] =
    logThread(
      for {
        p <- peers.get
        peer = p.get(nodeId).filter(pd => NodeState.isNotOffline(pd.peerMetadata.nodeState))
        majorityHeight <- LiftIO[F].liftIO(dao.redownloadService.latestMajorityHeight)
        _ <- peer.traverse(peer => ipManager.removeKnownIP(peer.peerMetadata.host))
        maxProposalHeight <- peer.traverse { peer =>
          LiftIO[F].liftIO(dao.redownloadService.getPeerProposals().map(_.get(peer.peerMetadata.id)))
        }.map(
          _.flatten
            .filter(_.nonEmpty)
            .map { _.maxBy { case (height, _) => height } }
            .map { case (height, _) => height }
        )
        leavingHeight = maxProposalHeight
          .map(height => if (height > majorityHeight) majorityHeight else height)
          .getOrElse(majorityHeight)
        _ <- peer.traverse(peer => {
          val peerData = peer
            .updateLeavingHeight(leavingHeight)
            .copy(peerMetadata = peer.peerMetadata.copy(nodeState = NodeState.Offline))
          peers.modify(p => (p + (peerData.peerMetadata.id -> peerData), p))
        })
        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_markOfflinePeer"
    )

  // TODO: not used but should be used for removing old peers
  def removePeer(pd: PeerData): F[Unit] =
    logThread(
      LiftIO[F]
        .liftIO(dao.redownloadService.lowestMajorityHeight)
        .map(height => pd.canBeRemoved(height))
        .ifM(
          for {
            p <- peers.get
            pm = pd.peerMetadata

            _ <- ipManager.removeKnownIP(pm.host)
            _ <- peers.modify(a => (a - pm.id, a - pm.id))
            // Technically we shouldn't remove it from eigenTrust if we want to keep the trained model for that node
            //        _ <- LiftIO[F].liftIO(dao.eigenTrust.unregisterAgent(pm.id))
            _ <- updateMetrics()
            _ <- updatePersistentStore()
          } yield (),
          F.unit
        ),
      "cluster_removePeer"
    )

  def internalHearthbeat(round: Long) =
    logThread(
      for {
        peers <- peers.get

        _ <- if (round % dao.processingConfig.peerHealthCheckInterval == 0) {
          peers.values.toList.traverse { pd =>
            apiClient.metrics
              .checkHealth()
              .run(pd.peerMetadata.toPeerClientMetadata)
              .flatTap { _ =>
                dao.metrics.incrementMetricAsync[F]("peerHealthCheckPassed")
              }
              .handleErrorWith { _ =>
                dao.metrics.incrementMetricAsync[F]("peerHealthCheckFailed") >>
                  setNodeStatus(pd.peerMetadata.id, NodeState.Offline)
              }
          }.void
        } else F.unit

        _ <- if (round % dao.processingConfig.peerHealthCheckInterval == 0) {
          peers.values.toList.traverse { pd =>
            peerDiscovery(pd.peerMetadata)
          }.void
        } else F.unit
      } yield (),
      "cluster_internalHearthbeat"
    )

  def initiateRejoin(): F[Unit] =
    logThread(
      for {
        _ <- if (dao.peersInfoPath.nonEmpty) {
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

  // Someone joins to me
  def pendingRegistrationRequest: F[PeerRegistrationRequest] =
    for {
      height <- getOwnJoinedHeight()

      peers <- peers.get.map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
      peersSize = peers.size
      minFacilitatorsSize = dao.processingConfig.numFacilitatorPeers
      isInitialFacilitator = peersSize < minFacilitatorsSize
      participatedInRollbackFlow <- participatedInRollbackFlow.get.map(_.getOrElse(false))
      participatedInGenesisFlow <- participatedInGenesisFlow.get.map(_.getOrElse(false))
      whitelistingHash = KryoSerializer.serializeAnyRef(dao.nodeConfig.whitelisting).sha256

      _ <- logger.debug(
        s"Pending registration request: ownHeight=$height peers=$peersSize isInitialFacilitator=$isInitialFacilitator participatedInRollbackFlow=$participatedInRollbackFlow participatedInGenesisFlow=$participatedInGenesisFlow"
      )

      prr <- F.delay {
        PeerRegistrationRequest(
          dao.externalHostString,
          dao.externalPeerHTTPPort,
          dao.id,
          ResourceInfo(
            diskUsableBytes = new java.io.File(dao.snapshotPath).getUsableSpace
          ),
          height,
          participatesInGenesisFlow = participatedInGenesisFlow,
          participatesInRollbackFlow = participatedInRollbackFlow,
          joinsAsInitialFacilitator = isInitialFacilitator,
          whitelistingHash
        )
      }
    } yield prr

  // I join to someone
  def attemptRegisterPeer(hp: HostPort): F[Unit] =
    logThread(
      withMetric(
        {
          dao.nodeConfig.whitelisting
            .get(hp.host)
            .map(PeerClientMetadata(hp.host, hp.port, _))
            .map {
              client =>
                apiClient.sign
                  .getRegistrationRequest()
                  .run(client)
                  .flatMap { registrationRequest =>
                    for {
                      _ <- pendingRegistration(hp.host, registrationRequest)
                      prr <- pendingRegistrationRequest
                      response <- apiClient.sign.register(prr).run(client)
                    } yield response
                  }
                  .handleErrorWith { err =>
                    logger.error(s"registration request failed: $err") >>
                      dao.metrics.incrementMetricAsync[F]("peerGetRegistrationRequestFailed") >>
                      err.raiseError[F, Unit]
                  }
            }
            .getOrElse(logger.error(s"unknown peer, not whitelisted"))

        },
        "addPeerWithRegistrationSymmetric"
      ),
      "cluster_attemptRegisterPeer"
    )

  def rejoin(): F[Unit] = {
    implicit val ec = ConstellationExecutionContext.bounded

    def attemptRejoin(lastKnownAPIClients: Seq[HostPort]): F[Unit] =
      lastKnownAPIClients match {
        case Nil => F.raiseError(new RuntimeException("Couldn't rejoin the cluster"))
        case hostPort :: remainingHostPorts =>
          join(hostPort)
            .handleErrorWith(err => {
              logger.error(s"Couldn't rejoin via ${hostPort.host}: ${err.getMessage}")
              attemptRejoin(remainingHostPorts)
            })
      }

    for {
      _ <- logger.warn("Trying to rejoin the cluster...")

      lastKnownPeers = parse(dao.peersInfoPath.lines.mkString)
        .map(_.as[Seq[PeerMetadata]])
        .toOption
        .flatMap(_.toOption)
        .getOrElse(Seq.empty[PeerMetadata])
        .map(pm => HostPort(pm.host, pm.httpPort))

      _ <- attemptRejoin(lastKnownPeers)
    } yield ()
  }

  private def clearServicesBeforeJoin(): F[Unit] =
    for {
      _ <- clearOwnJoinedHeight()
      _ <- LiftIO[F].liftIO(dao.blacklistedAddresses.clear)
      _ <- LiftIO[F].liftIO(dao.transactionChainService.clear)
      _ <- LiftIO[F].liftIO(dao.addressService.clear)
      _ <- LiftIO[F].liftIO(dao.soeService.clear)
      _ <- LiftIO[F].liftIO(dao.transactionService.clear)
      _ <- LiftIO[F].liftIO(dao.observationService.clear)
      _ <- LiftIO[F].liftIO(dao.redownloadService.clear)
      // TODO: add services to clear if needed
    } yield ()

  def join(hp: HostPort): F[Unit] =
    logThread(
      for {
        _ <- clearServicesBeforeJoin()
        _ <- attemptRegisterPeer(hp)
        _ <- T.sleep(15.seconds) >> broadcastOwnJoinedHeight()
        _ <- LiftIO[F].liftIO(dao.downloadService.download())
      } yield (),
      "cluster_join"
    )

  def leave(gracefulShutdown: => F[Unit]): F[Unit] =
    logThread(
      for {
        _ <- logger.info("Trying to gracefully leave the cluster")

        majorityHeight <- LiftIO[F].liftIO(dao.redownloadService.latestMajorityHeight)
        _ <- broadcastLeaveRequest(majorityHeight)
        _ <- compareAndSet(NodeState.all, NodeState.Leaving)

        // TODO: make interval check to wait for last n snapshots, set Offline state only in Leaving see #641 for details
        _ <- Timer[F].sleep(dao.processingConfig.leavingStandbyTimeout.seconds)

        _ <- compareAndSet(NodeState.all, NodeState.Offline)

        ips <- ipManager.listKnownIPs
        _ <- ips.toList.traverse(ipManager.removeKnownIP)
        _ <- peers.modify(_ => (Map.empty, Map.empty))
        _ <- if (dao.nodeConfig.isGenesisNode) setOwnJoinedHeight(0L) else clearOwnJoinedHeight()
        _ <- LiftIO[F].liftIO(dao.eigenTrust.clearAgents())
        _ <- updateMetrics()
        _ <- updatePersistentStore()

        _ <- gracefulShutdown
      } yield (),
      "cluster_leave"
    )

  def compareAndSet(expected: Set[NodeState], newState: NodeState, skipBroadcast: Boolean = false): F[SetStateResult] =
    nodeState.modify { current =>
      if (expected.contains(current)) (newState, SetStateResult(current, isNewSet = true))
      else (current, SetStateResult(current, isNewSet = false))
    }.flatTap(
        res =>
          if (res.isNewSet) LiftIO[F].liftIO(dao.metrics.updateMetricAsync[IO]("nodeState", newState.toString))
          else F.unit
      )
      .flatTap {
        case SetStateResult(_, true) if !skipBroadcast && broadcastStates.contains(newState) =>
          broadcastNodeState(newState)
        case _ => F.unit
      }

  private def broadcastLeaveRequest(majorityHeight: Long): F[Unit] = {
    def peerUnregister(c: PeerClientMetadata) =
      PeerUnregister(dao.peerHostPort.host, dao.peerHostPort.port, dao.id, majorityHeight)
    broadcast(c => apiClient.cluster.deregister(peerUnregister(c)).run(c)).void
  }

  def broadcastOfflineNodeState(nodeId: Id = dao.id): F[Unit] =
    broadcastNodeState(NodeState.Offline, nodeId)

  private def broadcastNodeState(nodeState: NodeState, nodeId: Id = dao.id): F[Unit] =
    logThread(
      broadcast(apiClient.cluster.setNodeStatus(SetNodeStatus(nodeId, nodeState)).run).flatTap {
        _.filter(_._2.isLeft).toList.traverse {
          case (id, e) => logger.warn(s"Unable to propagate status to node ID: $id")
        }
      }.void,
      "cluster_broadcastNodeState"
    )

  def broadcast[T](
    f: PeerClientMetadata => F[T],
    skip: Set[Id] = Set.empty,
    subset: Set[Id] = Set.empty
  ): F[Map[Id, Either[Throwable, T]]] =
    logThread(
      for {
        peerInfo <- peers.get.map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
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

  private def validWithLoopbackGuard(host: String): Boolean =
    (host != dao.externalHostString && host != "127.0.0.1" && host != "localhost") || !dao.preventLocalhostAsPeer

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
    metrics: () => Metrics,
    ipManager: IPManager[F],
    joiningPeerValidator: JoiningPeerValidator[F],
    apiClient: ClientInterpreter[F],
    dao: DAO
  ) = new Cluster(ipManager, joiningPeerValidator, apiClient, dao)

  case class ClusterNode(id: Id, ip: HostPort, status: NodeState, reputation: Long)
  case class TestClusterNode(aa: String)

  object ClusterNode {
    def apply(pm: PeerMetadata) = new ClusterNode(pm.id, HostPort(pm.host, pm.httpPort), pm.nodeState, 0L)
  }

}
