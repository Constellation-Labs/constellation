package org.constellation.p2p

import java.time.LocalDateTime

import cats.data.OptionT
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync, Timer}
import cats.implicits._
import cats.syntax._
import com.softwaremill.sttp.Response
import constellation._
import io.chrisdavenport.log4cats.Logger
import org.constellation.p2p.Cluster.ClusterNode
import org.constellation.p2p.PeerState.PeerState
import org.constellation.primitives.IPManager
import org.constellation.primitives.Schema.NodeState
import org.constellation.primitives.Schema.NodeState.{NodeState, broadcastStates}
import org.constellation.schema.Id
import org.constellation.util.Logging._
import org.constellation.util._
import org.constellation.{ConstellationExecutionContext, DAO, PeerMetadata}

import scala.concurrent.duration._
import scala.util.Random

case class SetNodeStatus(id: Id, nodeStatus: NodeState)
case class SetStateResult(oldState: NodeState, isNewSet: Boolean)
case class PeerData(
  peerMetadata: PeerMetadata,
  client: APIClient,
  notification: Seq[PeerNotification] = Seq.empty
)

case class PendingRegistration(ip: String, request: PeerRegistrationRequest)

case class Deregistration(ip: String, port: Int, id: Id)

case object GetPeerInfo

case class UpdatePeerInfo(peerData: PeerData)
case class ChangePeerState(id: Id, state: NodeState)

case class PeerNotification(id: Id, state: PeerState, timestamp: LocalDateTime = LocalDateTime.now()) extends Signable

object PeerState extends Enumeration {
  type PeerState = Value
  val Leave, Join = Value
}
case class UpdatePeerNotifications(notifications: Seq[PeerNotification])

class Cluster[F[_]: Concurrent: Logger: Timer: ContextShift](ipManager: IPManager[F], dao: DAO)(
  implicit C: ContextShift[F]
) {
  private val initialState: NodeState =
    if (dao.nodeConfig.cliConfig.startOfflineMode) NodeState.Offline else NodeState.PendingDownload
  private val nodeState: Ref[F, NodeState] = Ref.unsafe[F, NodeState](initialState)
  private val peers: Ref[F, Map[Id, PeerData]] = Ref.unsafe[F, Map[Id, PeerData]](Map.empty)

  implicit val shadedDao: DAO = dao

  dao.metrics.updateMetricAsync[IO]("nodeState", initialState.toString).unsafeRunAsync(_ => ())

  def getPeerInfo: F[Map[Id, PeerData]] = peers.get

  // TODO: wkoszycki consider using complex structure of peers so the lookup has O(n) complexity
  def getPeerData(host: String): F[Option[PeerData]] =
    peers.get
      .map(m => m.find(t => t._2.peerMetadata.host == host).map(_._2))

  def clusterNodes(): F[List[ClusterNode]] =
    getPeerInfo.map(_.values.toList.map(_.peerMetadata).map(ClusterNode(_))).flatMap { nodes =>
      getNodeState.map(ClusterNode(dao.id, dao.peerHostPort, _, 0L)).map(nodes ++ List(_))
    }

  def updatePeerNotifications(notifications: List[PeerNotification]): F[Unit] =
    logThread(
      for {
        p <- peers.get
        peerUpdate = notifications.flatMap { n =>
          p.get(n.id).map { p =>
            p.copy(notification = p.notification.diff(Seq(n)))
          }
        }
        _ <- Logger[F].debug(s"Peer update: ${peerUpdate}")
        _ <- peerUpdate.traverse(updatePeerInfo) // TODO: bulk update
      } yield (),
      "cluster_updatePeerNotifications"
    )

  def updatePeerInfo(peerData: PeerData): F[Unit] =
    logThread(
      for {
        _ <- peers.modify(p => (p + (peerData.client.id -> peerData), p))
        ip = peerData.client.hostName
        _ <- ipManager.addKnownIP(ip)
        _ <- Logger[F].debug(s"Added $ip to known peers.")
        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_updatePeerInfo"
    )

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
        } else Sync[F].unit

        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_removePeerRequest"
    )
  }

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
      Sync[F].delay(dao.peersInfoPath.write(p.values.toSeq.map(_.peerMetadata).json))
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
        APIClient(md._1.host, md._1.httpPort)(dao.backend, dao)
          .postNonBlockingUnitF[F]("register", dao.peerRegistrationRequest)(C)
      }.void
    } yield registerResponse

  def addPeerMetadata(pm: PeerMetadata): F[Unit] = {
    val validHost = (pm.host != dao.externalHostString && pm.host != "127.0.0.1") || !dao.preventLocalhostAsPeer

    if (pm.id == dao.id || !validHost) Sync[F].unit

    logThread(
      for {
        adjustedHost <- (if (pm.auxHost.nonEmpty) pm.auxHost else pm.host).pure[F]
        client = APIClient(adjustedHost, pm.httpPort)(dao.backend, dao)

        _ <- peerDiscovery(client)

        _ <- Sync[F].delay(client.id = pm.id)

        _ <- updatePeerInfo(PeerData(pm, client))
      } yield (),
      "cluster_addPeerMetadata"
    )
  }

  def pendingRegistration(ip: String, request: PeerRegistrationRequest): F[Unit] =
    logThread(
      for {
        pr <- PendingRegistration(ip, request).pure[F]
        _ <- Logger[F].debug(s"Pending Registration request: $pr")
        validExternalHost = validWithLoopbackGuard(request.host)
        peers <- peers.get
        existsNotOffline = peers.exists {
          case (_, d) =>
            d.client.hostName == request.host && d.client.apiPort == request.port && d.peerMetadata.nodeState != NodeState.Offline
        }

        isSelfId = dao.id == request.id
        badAttempt = isSelfId || !validExternalHost || existsNotOffline

        _ <- if (badAttempt) {
          dao.metrics.incrementMetricAsync[F]("duplicatePeerAdditionAttempt")
        } else {
          val client = APIClient(request.host, request.port)(dao.backend, dao)
          val authSignRequest = PeerAuthSignRequest(Random.nextLong())
          val req = client.postNonBlockingF[F, SingleHashSignature]("sign", authSignRequest)(C)

          req.flatTap { sig =>
            if (sig.hashSignature.id != request.id) {
              Logger[F].warn(s"Keys should be the same: ${sig.hashSignature.id} != ${request.id}") >>
                dao.metrics.incrementMetricAsync[F]("peerKeyMismatch")
            } else Sync[F].unit
          }.flatTap { sig =>
            if (!sig.valid) {
              Logger[F].warn(s"Invalid peer signature $request $authSignRequest $sig") >>
                dao.metrics.incrementMetricAsync[F]("invalidPeerRegistrationSignature")
            } else Sync[F].unit
          }.flatTap { sig =>
            dao.metrics.incrementMetricAsync[F]("peerAddedFromRegistrationFlow") >>
              Logger[F].debug(s"Valid peer signature $request $authSignRequest $sig")
          }.flatMap {
            sig =>
              val state = withMetric(
                client.getNonBlockingF[F, NodeStateInfo]("state")(C),
                "nodeState"
              )

              state.flatMap { s =>
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
                val peerData = PeerData(add, client)
                updatePeerInfo(peerData) >> C.shift >> implicitly[Concurrent[F]].start(peerDiscovery(client))
              }.void

          }.handleErrorWith { err =>
            Logger[F].warn(s"Sign request to ${request.host}:${request.port} failed. $err") >>
              dao.metrics.incrementMetricAsync[F]("peerSignatureRequestFailed")
          }
        }
      } yield (),
      "cluster_pendingRegistration"
    )

  def deregister(ip: String, port: Int, id: Id): F[Unit] =
    logThread(
      for {
        p <- peers.get
        _ <- p.get(id).traverse { peer =>
          updatePeerInfo(
            peer.copy(
              peerMetadata = peer.peerMetadata.copy(nodeState = NodeState.Leaving)
            )
          )
        }
      } yield (),
      "cluster_deregister"
    )

  // mwadon: Is it correct implementation?
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

  def markOfflinePeer(pd: PeerData): F[Unit] =
    logThread(
      for {
        p <- peers.get
        pm = pd.peerMetadata
        _ <- ipManager.removeKnownIP(pm.host)
        _ <- p
          .get(pm.id)
          .traverse(peer => {
            val peerData = peer.copy(peerMetadata = pm.copy(nodeState = NodeState.Offline))
            peers.modify(p => (p + (peerData.client.id -> peerData), p))
          })
        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_markOfflinePeer"
    )

  def removePeer(pd: PeerData): F[Unit] =
    logThread(
      for {
        p <- peers.get
        pm = pd.peerMetadata

        _ <- ipManager.removeKnownIP(pm.host)
        _ <- peers.modify(a => (a - pm.id, a - pm.id))

        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
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
        } else Sync[F].unit
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
        } else Sync[F].unit

        _ <- if (round % dao.processingConfig.peerHealthCheckInterval == 0) {
          peers.values.toList.traverse { d =>
            peerDiscovery(d.client)
          }.void
        } else Sync[F].unit
      } yield (),
      "cluster_internalHearthbeat"
    )

  def initiatePeerReload(): F[Unit] =
    logThread(
      for {
        _ <- if (dao.peersInfoPath.nonEmpty) {
          withMetric(
            dao.peersInfoPath.lines.mkString.x[Seq[PeerMetadata]].toList.traverse(addPeerMetadata).void,
            "peerReloading"
          )
        } else Sync[F].unit

        _ <- if (dao.seedsPath.nonEmpty) {
          withMetric(
            dao.seedsPath.lines.toList.traverse { line =>
              line.split(":") match {
                case Array(host, port) =>
                  hostPort(HostPort(host, port.toInt))
                case _ => Sync[F].unit
              }
            }.void,
            "reedSeedsFile"
          )
        } else Sync[F].unit

        _ <- Timer[F].sleep(15.seconds)

        _ <- if (dao.peersInfoPath.nonEmpty && dao.seedsPath.isEmpty) {
          Logger[F].warn(
            "Found existing peers in persistent storage. Node probably crashed before and will try to rejoin the cluster."
          )
          rejoin()
        } else {
          Logger[F].warn("No peers in persistent storage. Skipping rejoin.")
        }

        _ <- if (dao.seedsPath.nonEmpty && dao.peersInfoPath.isEmpty) {
          C.evalOn(ConstellationExecutionContext.bounded)(
            Sync[F].delay(Download.download()(dao, ConstellationExecutionContext.bounded))
          )
        } else {
          Logger[F].warn("No seeds configured yet. Skipping initial download.")
        }
      } yield (),
      "cluster_initiatePeerReload"
    )

  def attemptRegisterSelfWithPeer(hp: HostPort): F[Unit] =
    logThread(
      for {
        _ <- Logger[F].info(s"Attempting to register with $hp")
        _ <- withMetric(
          APIClient(hp.host, hp.port)(dao.backend, dao)
            .postNonBlockingUnitF("register", dao.peerRegistrationRequest)(C),
          "addPeerWithRegistration"
        )
      } yield (),
      "cluster_attemptRegisterSelfWithPeer"
    )

  def attemptRegisterPeer(hp: HostPort): F[Response[Unit]] =
    logThread(
      withMetric(
        {
          val client = APIClient(hp.host, hp.port)(dao.backend, dao)

          client
            .getNonBlockingF[F, PeerRegistrationRequest]("registration/request")(C)
            .flatMap { registrationRequest =>
              pendingRegistration(hp.host, registrationRequest) >>
                client.postNonBlockingUnitF("register", dao.peerRegistrationRequest)(C)
            }
            .handleErrorWith { err =>
              Logger[F].error(s"registration request failed: $err") >>
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
        case Nil => Sync[F].raiseError(new RuntimeException("Couldn't rejoin the cluster"))
        case hostPort :: remainingHostPorts =>
          join(hostPort)
            .handleErrorWith(err => {
              Logger[F].error(s"Couldn't rejoin via ${hostPort.host}: ${err.getMessage}")
              attemptRejoin(remainingHostPorts)
            })
      }

    for {
      _ <- Logger[F].warn("Trying to rejoin the cluster...")

      lastKnownPeers = dao.peersInfoPath.lines.mkString
        .x[Seq[PeerMetadata]]
        .map(pm => HostPort(pm.host, pm.httpPort))

      _ <- attemptRejoin(lastKnownPeers)
    } yield ()
  }

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

  def join(hp: HostPort): F[Unit] = {
    implicit val ec = ConstellationExecutionContext.bounded

    logThread(attemptRegisterPeer(hp) >> Sync[F].delay(Download.download()), "cluster_join")
  }

  def leave(gracefulShutdown: => F[Unit]): F[Unit] =
    logThread(
      for {
        _ <- Logger[F].info("Trying to gracefully leave the cluster")

        _ <- broadcastLeaveRequest()
        _ <- compareAndSet(NodeState.all, NodeState.Leaving)

        // TODO: make interval check to wait for last n snapshots, set Offline state only in Leaving see #641 for details
        _ <- Timer[F].sleep(dao.processingConfig.leavingStandbyTimeout.seconds)

        _ <- compareAndSet(NodeState.all, NodeState.Offline)

        ips <- ipManager.listKnownIPs
        _ <- ips.toList.traverse(ipManager.removeKnownIP)
        _ <- peers.modify(_ => (Map.empty, Map.empty))
        _ <- updateMetrics()
        _ <- updatePersistentStore()

        _ <- gracefulShutdown
      } yield (),
      "cluster_leave"
    )

  private def broadcastNodeState(nodeState: NodeState): F[Unit] =
    logThread(
      broadcast(_.postNonBlockingUnitF("status", SetNodeStatus(dao.id, nodeState))(C)).flatTap {
        _.filter(_._2.isLeft).toList.traverse {
          case (id, e) => Logger[F].warn(s"Unable to propagate status to node ID: $id")
        }
      }.void,
      "cluster_broadcastNodeState"
    )

  def compareAndSet(expected: Set[NodeState], newState: NodeState, skipBroadcast: Boolean = false): F[SetStateResult] =
    nodeState.modify { current =>
      if (expected.contains(current)) (newState, SetStateResult(current, isNewSet = true))
      else (current, SetStateResult(current, isNewSet = false))
    }.flatTap(
        res =>
          if (res.isNewSet) LiftIO[F].liftIO(dao.metrics.updateMetricAsync[IO]("nodeState", newState.toString))
          else Sync[F].unit
      )
      .flatTap {
        case SetStateResult(_, true) if !skipBroadcast && broadcastStates.contains(newState) =>
          broadcastNodeState(newState)
        case _ => Sync[F].unit
      }

  def getNodeState: F[NodeState] = nodeState.get

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

  private def broadcastLeaveRequest(): F[Unit] = {
    def peerUnregister(c: APIClient) = PeerUnregister(c.hostName, c.apiPort, c.id)
    broadcast(c => c.postNonBlockingUnitF("deregister", peerUnregister(c))(C)).void
  }

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

  def apply[F[_]: Concurrent: Logger: Timer: ContextShift](metrics: () => Metrics, ipManager: IPManager[F], dao: DAO) =
    new Cluster(ipManager, dao)

  case class ClusterNode(id: Id, ip: HostPort, status: NodeState, reputation: Long)

  object ClusterNode {
    def apply(pm: PeerMetadata) = new ClusterNode(pm.id, HostPort(pm.host, pm.httpPort), pm.nodeState, 0L)
  }

}
