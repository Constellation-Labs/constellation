package org.constellation.p2p

import java.time.LocalDateTime

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Timer}
import cats.implicits._
import com.softwaremill.sttp.Response
import constellation._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation._
import org.constellation.domain.redownload.RedownloadService.LatestMajorityHeight
import org.constellation.p2p.Cluster.ClusterNode
import org.constellation.p2p.PeerState.PeerState
import org.constellation.primitives.IPManager
import org.constellation.primitives.Schema.NodeState
import org.constellation.primitives.Schema.NodeState.{NodeState, broadcastStates}
import org.constellation.rollback.{
  CannotLoadGenesisObservationFile,
  CannotLoadSnapshotInfoFile,
  CannotLoadSnapshotsFiles
}
import org.constellation.schema.Id
import org.constellation.util.Logging._
import org.constellation.util._

import scala.concurrent.duration._
import scala.util.Random

case class SetNodeStatus(id: Id, nodeStatus: NodeState)
case class SetStateResult(oldState: NodeState, isNewSet: Boolean)
case class PeerData(
  peerMetadata: PeerMetadata,
  client: APIClient,
  majorityHeight: MajorityHeight,
  notification: Seq[PeerNotification] = Seq.empty
)

case class PendingRegistration(ip: String, request: PeerRegistrationRequest)

case class Deregistration(ip: String, port: Int, id: Id)

case class JoinedHeight(id: Id, height: Long)

case object GetPeerInfo

case class UpdatePeerInfo(peerData: PeerData)
case class ChangePeerState(id: Id, state: NodeState)

case class PeerNotification(id: Id, state: PeerState, timestamp: LocalDateTime = LocalDateTime.now()) extends Signable

case class MajorityHeight(joined: Option[Long], left: Option[Long] = None)

object MajorityHeight {
  def genesis: MajorityHeight = MajorityHeight(Some(0L), None)
}

object PeerState extends Enumeration {
  type PeerState = Value
  val Leave, Join = Value
}
case class UpdatePeerNotifications(notifications: Seq[PeerNotification])

class Cluster[F[_]](
  ipManager: IPManager[F],
  joiningPeerValidator: JoiningPeerValidator[F],
  dao: DAO
)(
  implicit F: Concurrent[F],
  C: ContextShift[F],
  T: Timer[F]
) {

  val ownJoinedHeight: Ref[F, Option[Long]] = Ref.unsafe[F, Option[Long]] {
    if (dao.nodeConfig.isGenesisNode) Some(0L) else None
  }
  private val initialState: NodeState =
    if (dao.nodeConfig.cliConfig.startOfflineMode) NodeState.Offline else NodeState.PendingDownload
  private val nodeState: Ref[F, NodeState] = Ref.unsafe[F, NodeState](initialState)
  private val peers: Ref[F, Map[Id, PeerData]] = Ref.unsafe[F, Map[Id, PeerData]](Map.empty)

  implicit val logger = Slf4jLogger.getLogger[F]

  implicit val shadedDao: DAO = dao

  dao.metrics.updateMetricAsync[IO]("nodeState", initialState.toString).unsafeRunAsync(_ => ())
  private val stakingAmount = ConfigUtil.getOrElse("constellation.staking-amount", 0L)

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
              peerData.copy(majorityHeight = peerData.majorityHeight.copy(joined = Some(joinedHeight.height)))
            ),
            ()
          )
        case _ => (m, ())
      }
    }
  }

  def removePeerRequest(hp: Option[HostPort], id: Option[Id]): F[Unit] = {
    def filter(p: Map[Id, PeerData]) = p.filter {
      case (pid, d) =>
        val badHost = hp.exists {
          case HostPort(host, port) =>
            d.client.hostName == host && d.client.apiPort == port
        }
        val badId = id.contains(pid)
        !badHost && !badId
    }

    logThread(
      for {
        isRemoved <- peers.modify { p =>
          val filtered = filter(p)
          (filtered, filtered != p)
        }
        _ <- if (isRemoved) {
          dao.metrics.incrementMetricAsync[F]("peerRemoved")
        } else F.unit

        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_removePeerRequest"
    )
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

  def peerDiscovery(client: APIClient): F[Unit] =
    for {
      peersMetadata <- client.getNonBlockingF[F, Seq[PeerMetadata]]("peers")(C).handleErrorWith { err =>
        dao.metrics.incrementMetricAsync[F]("peerDiscoveryQueryFailed") >> err.raiseError[F, Seq[PeerMetadata]]
      }
      peers <- getPeerInfo
      filteredPeers = peersMetadata.filter { p =>
        p.id != dao.id && validPeerAddition(HostPort(p.host, p.httpPort), peers)
      }
      register <- filteredPeers.toList.traverse { md =>
        APIClient(md.host, md.httpPort)(dao.backend, dao)
          .getNonBlockingF[F, PeerRegistrationRequest]("registration/request")(C)
          .map((md, _))
      }
      _ <- register.traverse(r => pendingRegistration(r._1.host, r._2))
      registerResponse <- register.traverse { md =>
        for {
          prr <- pendingRegistrationRequest
          _ <- APIClient(md._1.host, md._1.httpPort)(dao.backend, dao).postNonBlockingUnitF[F]("register", prr)(C)
        } yield ()
      }.void
    } yield registerResponse

  def addPeerMetadata(pm: PeerMetadata): F[Unit] = {
    val validHost = (pm.host != dao.externalHostString && pm.host != "127.0.0.1") || !dao.preventLocalhostAsPeer

    if (pm.id == dao.id || !validHost) F.unit

    logThread(
      for {
        adjustedHost <- (if (pm.auxHost.nonEmpty) pm.auxHost else pm.host).pure[F]
        client = APIClient(adjustedHost, pm.httpPort)(dao.backend, dao)

        _ <- peerDiscovery(client)

        _ <- F.delay(client.id = pm.id)

        _ <- updatePeerInfo(PeerData(pm, client, MajorityHeight(None))) // TODO: mwadon
      } yield (),
      "cluster_addPeerMetadata"
    )
  }

  def pendingRegistration(ip: String, request: PeerRegistrationRequest): F[Unit] =
    logThread(
      for {
        pr <- PendingRegistration(ip, request).pure[F]
        _ <- logger.debug(s"Pending Registration request: $pr")
        validExternalHost = validWithLoopbackGuard(request.host)
        peers <- peers.get
        existsNotOffline = peers.exists {
          case (_, d) =>
            d.client.hostName == request.host && d.client.apiPort == request.port && NodeState.canActAsJoiningSource(
              d.peerMetadata.nodeState
            )
        }

        isSelfId = dao.id == request.id
        badAttempt = isSelfId || !validExternalHost || existsNotOffline
        isValid <- joiningPeerValidator.isValid(APIClient(request.host, request.port - 1)(dao.backend, dao))

        _ <- if (badAttempt) {
          dao.metrics.incrementMetricAsync[F]("duplicatePeerAdditionAttempt")
        } else if (!isValid) {
          dao.metrics.incrementMetricAsync[F]("invalidPeerAdditionAttempt")
        } else {
          val client = APIClient(request.host, request.port)(dao.backend, dao)
          val authSignRequest = PeerAuthSignRequest(Random.nextLong())
          val req = client.postNonBlockingF[F, SingleHashSignature]("sign", authSignRequest)(C)

          req.flatTap { sig =>
            if (sig.hashSignature.id != request.id) {
              logger.warn(s"Keys should be the same: ${sig.hashSignature.id} != ${request.id}") >>
                dao.metrics.incrementMetricAsync[F]("peerKeyMismatch")
            } else F.unit
          }.flatTap { sig =>
            if (!sig.valid) {
              logger.warn(s"Invalid peer signature $request $authSignRequest $sig") >>
                dao.metrics.incrementMetricAsync[F]("invalidPeerRegistrationSignature")
            } else F.unit
          }.flatTap { sig =>
            dao.metrics.incrementMetricAsync[F]("peerAddedFromRegistrationFlow") >>
              logger.debug(s"Valid peer signature $request $authSignRequest $sig")
          }.flatMap {
            sig =>
              val state = withMetric(
                client.getNonBlockingF[F, NodeStateInfo]("state")(C),
                "nodeState"
              )

              state.flatMap {
                s =>
                  val id = sig.hashSignature.id
                  val add = PeerMetadata(
                    request.host,
                    request.port,
                    id,
                    nodeState = s.nodeState,
                    auxAddresses = s.addresses,
                    resourceInfo = request.resourceInfo
                  )
                  client.id = id
                  val peerData = PeerData(add, client, MajorityHeight(request.majorityHeight))

                  val updateJoiningHeight =
                    if (request.isGenesis)
                      ownJoinedHeight.modify(_ => (Some(0L), ())) >> logger.debug("Joining as genesis peer")
                    else F.unit

                  updatePeerInfo(peerData) >> updateJoiningHeight >> C.shift >> F.start(peerDiscovery(client))
              }.void

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
      p <- peers.get
      clients = p.map(_._2.client).toList
      maxMajorityHeight <- clients
        .traverse(_.getNonBlockingF[F, LatestMajorityHeight]("latestMajorityHeight")(C))
        .map(heights => if (heights.nonEmpty) heights.map(_.highest).max else 0L)
      delay = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")
      ownHeight = maxMajorityHeight + delay
      _ <- ownJoinedHeight.modify(_ => (Some(ownHeight), ()))
    } yield ownHeight

    for {
      height <- ownJoinedHeight.get
      _ <- logger.debug(s"Broadcasting own joined height - step1: height=$height")
      ownHeight <- height.map(_.pure[F]).getOrElse(discoverJoinedHeight)
      _ <- logger.debug(s"Broadcasting own joined height - step2: height=$ownHeight")
      _ <- broadcast(_.postNonBlockingUnitF("joinedHeight", JoinedHeight(dao.id, ownHeight))(C))
    } yield ()
  }

  def deregister(peerUnregister: PeerUnregister): F[Unit] =
    logThread(
      for {
        p <- peers.get
        _ <- p.get(peerUnregister.id).traverse { peer =>
          updatePeerInfo(
            peer.copy(
              peerMetadata = peer.peerMetadata.copy(nodeState = NodeState.Leaving),
              majorityHeight = peer.majorityHeight.copy(left = Some(peerUnregister.majorityHeight))
            )
          )
        }
      } yield (),
      "cluster_deregister"
    )

  def updatePeerInfo(peerData: PeerData): F[Unit] =
    logThread(
      for {
        _ <- peers.modify(p => (p + (peerData.client.id -> peerData), p))
        ip = peerData.client.hostName
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
            val addr = s"http://${clientI.client.hostName}:${clientI.client.apiPort - 1}"
            s"${idI.short} API: $addr"
        }.mkString(" --- ")
      )
    }

  private def updatePersistentStore(): F[Unit] =
    peers.get.flatMap { p =>
      F.delay(dao.peersInfoPath.write(p.values.toSeq.map(_.peerMetadata).json))
    }

  // mwadon: Is it correct implementation?
  // TODO: Unused method?
  def forgetPeer(pd: PeerData): F[Unit] =
    logThread(
      for {
        p <- peers.get
        pm = pd.peerMetadata
        _ <- ipManager.removeKnownIP(pm.host)
        _ <- p.get(pm.id).traverse { peer =>
          val peerData = peer.copy(notification = peer.notification ++ Seq(PeerNotification(pm.id, PeerState.Leave)))
          peers.modify(p => (p + (peerData.client.id -> peerData), p))
        }
        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_forgetPeer"
    )

  def markOfflinePeer(nodeId: Id): F[Unit] =
    logThread(
      for {
        p <- peers.get
        peer = p.get(nodeId).filter(pd => pd.peerMetadata.nodeState != NodeState.Offline)
        majorityHeight <- LiftIO[F].liftIO(dao.redownloadService.latestMajorityHeight)
        _ <- peer
          .traverse(peer => ipManager.removeKnownIP(peer.peerMetadata.host))
        _ <- peer
          .traverse(peer => {
            val peerData = peer.copy(
              peerMetadata = peer.peerMetadata.copy(nodeState = NodeState.Offline),
              majorityHeight = peer.majorityHeight.copy(left = Some(majorityHeight))
            )
            peers.modify(p => (p + (peerData.client.id -> peerData), p))
          })
        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_markOfflinePeer"
    )

  def removePeer(pd: PeerData): F[Unit] =
    logThread(
      LiftIO[F]
        .liftIO(dao.redownloadService.lowestMajorityHeight)
        .map(height => pd.majorityHeight.left.exists(_ > height))
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

  def hostPort(hp: HostPort): F[Unit] =
    logThread(
      for {
        peers <- peers.get
        _ <- if (!peers.exists {
                   case (_, data) => data.peerMetadata.host == hp.host && data.peerMetadata.httpPort == hp.port
                 } && validWithLoopbackGuard(hp.host)) {
          attemptRegisterPeer(hp).void
        } else F.unit
      } yield (),
      "cluster_hostPort"
    )

  def internalHearthbeat(round: Long) =
    logThread(
      for {
        peers <- peers.get

        _ <- if (round % dao.processingConfig.peerHealthCheckInterval == 0) {
          peers.values.toList.traverse { d =>
            d.client.getStringF("health")(C).flatTap { x =>
              if (x.isSuccess) {
                dao.metrics.incrementMetricAsync[F]("peerHealthCheckPassed")
              } else {
                dao.metrics.incrementMetricAsync[F]("peerHealthCheckFailed") >>
                  setNodeStatus(d.peerMetadata.id, NodeState.Offline)
              }
            }
          }.void
        } else F.unit

        _ <- if (round % dao.processingConfig.peerHealthCheckInterval == 0) {
          peers.values.toList.traverse { d =>
            peerDiscovery(d.client)
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

  def attemptRegisterSelfWithPeer(hp: HostPort): F[Unit] =
    logThread(
      for {
        _ <- logger.info(s"Attempting to register with $hp")
        prr <- pendingRegistrationRequest
        _ <- withMetric(
          APIClient(hp.host, hp.port)(dao.backend, dao)
            .postNonBlockingUnitF("register", prr)(C),
          "addPeerWithRegistration"
        )
      } yield (),
      "cluster_attemptRegisterSelfWithPeer"
    )

  def pendingRegistrationRequest: F[PeerRegistrationRequest] =
    for {
      height <- ownJoinedHeight.get

      peers <- peers.get
      peersSize = peers.size
      minFacilitatorsSize = dao.processingConfig.numFacilitatorPeers
      isGenesis = peersSize < minFacilitatorsSize

      _ <- logger.debug(
        s"Pending registration request: ownHeight=$height peers=$peersSize isGenesis=$isGenesis"
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
          isGenesis = isGenesis
        )
      }
    } yield prr

  def attemptRegisterPeer(hp: HostPort): F[Response[Unit]] =
    logThread(
      withMetric(
        {
          val client = APIClient(hp.host, hp.port)(dao.backend, dao)

          client
            .getNonBlockingF[F, PeerRegistrationRequest]("registration/request")(C)
            .flatMap { registrationRequest =>
              for {
                _ <- pendingRegistration(hp.host, registrationRequest)
                prr <- pendingRegistrationRequest
                response <- client.postNonBlockingUnitF("register", prr)(C)
              } yield response
            }
            .handleErrorWith { err =>
              logger.error(s"registration request failed: $err") >>
                dao.metrics.incrementMetricAsync[F]("peerGetRegistrationRequestFailed") >>
                err.raiseError[F, Response[Unit]]
            }
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

      lastKnownPeers = dao.peersInfoPath.lines.mkString
        .x[Seq[PeerMetadata]]
        .map(pm => HostPort(pm.host, pm.httpPort))

      _ <- attemptRejoin(lastKnownPeers)
    } yield ()
  }

  def attemptRollback(): F[Unit] =
    LiftIO[F]
      .liftIO(dao.rollbackService.validateAndRestore().value)
      .flatMap(
        _.fold(
          { err =>
            err match {
              case CannotLoadSnapshotInfoFile(path) =>
                logger.warn(s"Node has no SnapshotInfo backup in path: ${path}. Skipping the rollback.")
              case CannotLoadSnapshotsFiles(path) =>
                logger.warn(s"Node has no Snapshots backup in path: ${path}. Skipping the rollback.")
              case CannotLoadGenesisObservationFile(path) =>
                logger.warn(s"Node has no Genesis observation backup in path: ${path}. Skipping the rollback.")
              case _ => logger.error(s"Rollback failed: ${err}")
            }
          },
          _ => Logger[F].warn(s"Performed rollback.")
        )
      )

  def addToPeer(hp: HostPort): F[Response[Unit]] =
    logThread(
      withMetric(
        {
          val client = APIClient(hp.host, hp.port)(dao.backend, dao)

          client.postNonBlockingUnitF("peer/add", dao.peerHostPort)(C)
        },
        "addToPeer"
      ),
      "cluster_addToPeer"
    )

  private def clearServicesBeforeJoin(): F[Unit] =
    for {
      _ <- ownJoinedHeight.modify(_ => (None, ()))
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
        _ <- F.start(T.sleep(30.seconds) >> broadcastOwnJoinedHeight())
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
        _ <- ownJoinedHeight.modify(_ => (if (dao.nodeConfig.isGenesisNode) Some(0L) else None, ()))
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
    def peerUnregister(c: APIClient) = PeerUnregister(c.hostName, c.apiPort, dao.id, majorityHeight)
    broadcast(c => c.postNonBlockingUnitF("deregister", peerUnregister(c))(C)).void
  }

  def broadcastOfflineNodeState(nodeId: Id = dao.id): F[Unit] =
    broadcastNodeState(NodeState.Offline, nodeId)

  private def broadcastNodeState(nodeState: NodeState, nodeId: Id = dao.id): F[Unit] =
    logThread(
      broadcast(_.postNonBlockingUnitF("status", SetNodeStatus(nodeId, nodeState))(C)).flatTap {
        _.filter(_._2.isLeft).toList.traverse {
          case (id, e) => logger.warn(s"Unable to propagate status to node ID: $id")
        }
      }.void,
      "cluster_broadcastNodeState"
    )

  def broadcast[T](
    f: APIClient => F[T],
    skip: Set[Id] = Set.empty,
    subset: Set[Id] = Set.empty
  ): F[Map[Id, Either[Throwable, T]]] =
    logThread(
      for {
        peerInfo <- peers.get
        selected = if (subset.nonEmpty) {
          peerInfo.filterKeys(subset.contains)
        } else {
          peerInfo.filterKeys(id => !skip.contains(id))
        }
        (keys, values) = selected.toList.unzip
        res <- values
          .map(_.client)
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
      case (_, data) => data.client.hostName == hp.host && data.client.apiPort == hp.port
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
    dao: DAO
  ) = new Cluster(ipManager, joiningPeerValidator, dao)

  case class ClusterNode(id: Id, ip: HostPort, status: NodeState, reputation: Long)

  object ClusterNode {
    def apply(pm: PeerMetadata) = new ClusterNode(pm.id, HostPort(pm.host, pm.httpPort), pm.nodeState, 0L)
  }

}
