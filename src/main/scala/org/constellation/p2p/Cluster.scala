package org.constellation.p2p

import java.time.LocalDateTime

import cats.effect.{Concurrent, ContextShift, Sync, Timer}
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.config.Config
import constellation._
import io.chrisdavenport.log4cats.Logger
import org.constellation.p2p.PeerState.PeerState
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.{Id, NodeState}
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.primitives.{IPManager, Schema}
import org.constellation.util._
import org.constellation.{ConstellationExecutionContext, DAO, PeerMetadata}

import scala.concurrent.duration._
import scala.util.Random

case class SetNodeStatus(id: Id, nodeStatus: NodeState)

case class PeerData(
  peerMetadata: PeerMetadata,
  client: APIClient,
  notification: Seq[PeerNotification] = Seq.empty
)

case class PeerHealthCheck(status: Map[Id, Boolean])

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
  private val nodeState: SingleRef[F, NodeState] = SingleRef[F, NodeState](initialState)
  private val peers: SingleRef[F, Map[Id, PeerData]] = SingleRef[F, Map[Id, PeerData]](Map.empty)

  def isNodeReady: F[Boolean] = nodeState.get.map(_ == NodeState.Ready)

  def getPeerInfo: F[Map[Id, PeerData]] = peers.get

  def updatePeerNotifications(notifications: List[PeerNotification]): F[Unit] =
    for {
      p <- peers.getUnsafe
      peerUpdate = notifications.flatMap { n =>
        p.get(n.id).map { p =>
          p.copy(notification = p.notification.diff(Seq(n)))
        }
      }
      _ <- Logger[F].debug(s"Peer update: ${peerUpdate}")
      _ <- peerUpdate.traverse(updatePeerInfo) // TODO: bulk update
    } yield ()

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
    } yield ()
  }

  def setNodeStatus(id: Id, state: NodeState): F[Unit] =
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
    } yield ()

  def peerDiscovery(client: APIClient): F[Unit] =
    for {
      peersMetadata <- client.getNonBlockingF[F, Seq[PeerMetadata]]("peers").handleErrorWith { err =>
        dao.metrics.incrementMetricAsync[F]("peerDiscoveryQueryFailed") *> err.raiseError[F, Seq[PeerMetadata]]
      }
      peers <- getPeerInfo
      filteredPeers = peersMetadata.filter { p =>
        p.id != dao.id && validPeerAddition(HostPort(p.host, p.httpPort), peers)
      }
      register <- filteredPeers.toList.traverse { md =>
        APIClient(md.host, md.httpPort)(ConstellationExecutionContext.callbacks)
          .getNonBlockingF[F, PeerRegistrationRequest]("registration/request")
          .map((md, _))
      }
      _ <- register.traverse(r => pendingRegistration(r._1.host, r._2))
      registerResponse <- register.traverse { md =>
        APIClient(md._1.host, md._1.httpPort)(ConstellationExecutionContext.callbacks)
          .postNonBlockingUnitF[F]("register", dao.peerRegistrationRequest)
      }.void
    } yield registerResponse

  def addPeerMetadata(pm: PeerMetadata): F[Unit] = {
    val validHost = (pm.host != dao.externalHostString && pm.host != "127.0.0.1") || !dao.preventLocalhostAsPeer

    if (pm.id == dao.id || !validHost) Sync[F].unit

    for {
      adjustedHost <- (if (pm.auxHost.nonEmpty) pm.auxHost else pm.host).pure[F]
      client = APIClient(adjustedHost, pm.httpPort)(ConstellationExecutionContext.callbacks, dao)

      _ <- C.evalOn(ConstellationExecutionContext.callbacks)(peerDiscovery(client))

      _ <- Sync[F].delay(client.id = pm.id)

      _ <- updatePeerInfo(PeerData(pm, client))
    } yield ()
  }

  def pendingRegistration(ip: String, request: PeerRegistrationRequest): F[Unit] =
    for {
      pr <- PendingRegistration(ip, request).pure[F]
      _ <- Logger[F].debug(s"Pending Registration request: $pr")
      validExternalHost = validWithLoopbackGuard(request.host)
      peers <- peers.get
      hostAlreadyExists = peers.exists {
        case (_, d) => d.client.hostName == request.host && d.client.apiPort == request.port
      }
      validHost = validExternalHost && !hostAlreadyExists
      isSelfId = dao.id == request.id
      badAttempt = isSelfId || !validHost

      _ <- if (badAttempt) {
        dao.metrics.incrementMetricAsync[F]("duplicatePeerAdditionAttempt")
      } else {
        val client = APIClient(request.host, request.port)(ConstellationExecutionContext.callbacks, dao)
        val authSignRequest = PeerAuthSignRequest(Random.nextLong())
        val req = client.postNonBlockingF[F, SingleHashSignature]("sign", authSignRequest)

        req.flatTap { sig =>
          if (sig.hashSignature.id != request.id) {
            Logger[F].warn(s"Keys should be the same: ${sig.hashSignature.id} != ${request.id}") *>
              dao.metrics.incrementMetricAsync[F]("peerKeyMismatch")
          } else Sync[F].unit
        }.flatTap { sig =>
          if (!sig.valid) {
            Logger[F].warn(s"Invalid peer signature $request $authSignRequest $sig") *>
              dao.metrics.incrementMetricAsync[F]("invalidPeerRegistrationSignature")
          } else Sync[F].unit
        }.flatTap { sig =>
          dao.metrics.incrementMetricAsync[F]("peerAddedFromRegistrationFlow") *>
            Logger[F].debug(s"Valid peer signature $request $authSignRequest $sig")
        }.flatMap { sig =>
          val state = withMetric(
            client.getNonBlockingF[F, NodeStateInfo]("state"),
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
            updatePeerInfo(peerData) *> C.shift *> peerDiscovery(client) // TODO: make in background
          }.void

        }.handleErrorWith { err =>
          Logger[F].warn(s"Sign request to ${request.host}:${request.port} failed. $err") *>
            dao.metrics.incrementMetricAsync[F]("peerSignatureRequestFailed")
        }
      }
    } yield ()

  def deregister(ip: String, port: Int, id: Id): F[Unit] =
    for {
      p <- peers.getUnsafe
      _ <- p.get(id).traverse { peer =>
        updatePeerInfo(
          peer.copy(
            peerMetadata = peer.peerMetadata.copy(nodeState = NodeState.Leaving)
          )
        )
      }
    } yield ()

  def forgetPeer(pd: PeerData): F[Unit] =
    for {
      p <- peers.getUnsafe
      pm = pd.peerMetadata
      _ <- ipManager.removeKnownIP(pm.host)
      _ <- p.get(pm.id).traverse { peer =>
        updatePeerInfo(
          peer.copy(notification = peer.notification ++ Seq(PeerNotification(pm.id, PeerState.Leave)))
        )
      }
    } yield ()

  def updatePeerInfo(peerData: PeerData): F[Unit] =
    for {
      _ <- peers.modify(p => (p + (peerData.client.id -> peerData), p))
      ip = peerData.client.hostName
      _ <- ipManager.addKnownIP(ip)
      _ <- Logger[F].info(s"Added $ip to known peers.")
      _ <- updateMetrics()
      _ <- updatePersistentStore()
    } yield ()

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

  def hostPort(hp: HostPort): F[Unit] =
    for {
      peers <- peers.get
      _ <- if (!peers.exists {
                 case (_, data) => data.peerMetadata.host == hp.host && data.peerMetadata.httpPort == hp.port
               } && validWithLoopbackGuard(hp.host)) {
        attemptRegisterPeer(hp).void
      } else Sync[F].unit
    } yield ()

  def internalHearthbeat(round: Long) =
    for {
      peers <- peers.get

      _ <- if (round % dao.processingConfig.peerHealthCheckInterval == 0) {
        peers.values.toList.traverse { d =>
          d.client.getStringF("health").flatTap { x =>
            if (x.isSuccess) {
              dao.metrics.incrementMetricAsync[F]("peerHealthCheckPassed")
            } else {
              dao.metrics.incrementMetricAsync[F]("peerHealthCheckFailed") *>
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
    } yield ()

  def initiatePeerReload(): F[Unit] =
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

      _ <- if (dao.peersInfoPath.nonEmpty || dao.seedsPath.nonEmpty) {
        C.evalOn(ConstellationExecutionContext.callbacks)(
          Sync[F].delay(Download.download()(dao, ConstellationExecutionContext.bounded))
        )
      } else {
        Logger[F].warn("No peers or seeds configured yet. Skipping initial download.")
      }
    } yield ()

  def attemptRegisterSelfWithPeer(hp: HostPort): F[Unit] =
    for {
      _ <- Logger[F].info(s"Attempting to register with $hp")
      _ <- withMetric(
        APIClient(hp.host, hp.port)(ConstellationExecutionContext.callbacks, dao)
          .postNonBlockingUnitF("register", dao.peerRegistrationRequest),
        "addPeerWithRegistration"
      )
    } yield ()

  // TODO: move outside this class
  def withMetric[A](fa: F[A], prefix: String): F[A] =
    fa.flatTap(_ => dao.metrics.incrementMetricAsync[F](s"${prefix}_success")).handleErrorWith { err =>
      dao.metrics.incrementMetricAsync[F](s"${prefix}_failure") *> err.raiseError[F, A]
    }

  def attemptRegisterPeer(hp: HostPort): F[Response[Unit]] =
    withMetric(
      {
        val client = APIClient(hp.host, hp.port)(ConstellationExecutionContext.callbacks, dao)

        client
          .getNonBlockingF[F, PeerRegistrationRequest]("registration/request")
          .flatMap { registrationRequest =>
            pendingRegistration(hp.host, registrationRequest) *>
              client.postNonBlockingUnitF("register", dao.peerRegistrationRequest)
          }
          .handleErrorWith { err =>
            Logger[F].error(s"registration request failed: $err") *>
              dao.metrics.incrementMetricAsync[F]("peerGetRegistrationRequestFailed") *>
              err.raiseError[F, Response[Unit]]
          }
      },
      "addPeerWithRegistrationSymmetric"
    )

  def addToPeer(hp: HostPort): F[Response[Unit]] =
    withMetric(
      {
        val client = APIClient(hp.host, hp.port)(ConstellationExecutionContext.callbacks, dao)

        client.postNonBlockingUnitF("peer/add", dao.peerHostPort)
      },
      "addToPeer"
    )

  def broadcastNodeState(): F[Unit] =
    getNodeState.flatMap { nodeState =>
      broadcast(_.postNonBlockingUnitF("status", SetNodeStatus(dao.id, nodeState)))
    }.flatTap {
      _.filter(_._2.isLeft).toList.traverse {
        case (id, e) => Logger[F].warn(s"Unable to propagate status to node ID: $id")
      }
    }.void

  def getNodeState: F[NodeState] = nodeState.get

  def broadcast[T](
    f: APIClient => F[T],
    skip: Set[Id] = Set.empty,
    subset: Set[Id] = Set.empty
  ): F[Map[Id, Either[Throwable, T]]] =
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
    } yield res

  def join(hp: HostPort): F[Unit] = {
    implicit val shadedDAO: DAO = dao
    implicit val ec = ConstellationExecutionContext.bounded

    attemptRegisterPeer(hp) *> Sync[F].delay(Download.download())
  }

  def leave(gracefulShutdown: => F[Unit]): F[Unit] =
    for {
      _ <- Logger[F].info("Trying to gracefully leave the cluster")

      _ <- C.evalOn(ConstellationExecutionContext.callbacks)(broadcastLeaveRequest())
      _ <- setNodeState(NodeState.Leaving)
      _ <- broadcastNodeState()

      // TODO: use wkoszycki changes for waiting for last n snapshots
      _ <- Timer[F].sleep(dao.processingConfig.leavingStandbyTimeout.seconds)

      _ <- setNodeState(NodeState.Offline)
      _ <- broadcastNodeState()

      _ <- gracefulShutdown
    } yield ()

  private def broadcastLeaveRequest(): F[Unit] = {
    def peerUnregister(c: APIClient) = PeerUnregister(c.hostName, c.apiPort, c.id)
    broadcast(c => c.postNonBlockingUnitF("deregister", peerUnregister(c))).void
  }

  def setNodeState(state: NodeState): F[Unit] =
    nodeState.modify(oldState => (state, oldState)).flatMap { oldState =>
      Logger[F].debug(s"Changing node state from $oldState to $state")
    } *> dao.metrics.updateMetricAsync[F]("nodeState", state.toString)

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

  type Peers = Map[Schema.Id, PeerData]

  def apply[F[_]: Concurrent: Logger: Timer: ContextShift](metrics: () => Metrics, ipManager: IPManager[F], dao: DAO) =
    new Cluster(ipManager, dao)

  def loadSeedsFromConfig(config: Config): Seq[HostPort] =
    if (config.hasPath("seedPeers")) {
      import scala.collection.JavaConverters._
      val peersList = config.getStringList("seedPeers")
      peersList.asScala
        .map(_.split(":"))
        .map(arr => HostPort(arr(0), arr(1).toInt))
    } else Seq()

}
