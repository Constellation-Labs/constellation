package org.constellation.primitives

import akka.actor.{Actor, ActorSystem}
import akka.stream.ActorMaterializer
import cats.data.ValidatedNel
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import constellation.{futureTryWithTimeoutMetric, _}
import org.constellation.p2p.{Download, PeerAuthSignRequest, PeerRegistrationRequest}
import org.constellation.primitives.PeerState.PeerState
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.{Id, InternalHeartbeat, NodeState}
import org.constellation.util.Validation._
import org.constellation.util._
import org.constellation.{DAO, PeerMetadata, RemovePeerRequest}
import org.joda.time.LocalDateTime

import scala.collection.Set
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Random, Success, Try}

case class SetNodeStatus(id: Id, nodeStatus: NodeState)

object PeerManager extends StrictLogging {

  type Peers = Map[Schema.Id, PeerData]

  def loadSeedsFromConfig(config: Config): Seq[HostPort] =
    if (config.hasPath("seedPeers")) {
      import scala.collection.JavaConverters._
      val peersList = config.getStringList("seedPeers")
      peersList.asScala
        .map(_.split(":"))
        .map(arr => HostPort(arr(0), arr(1).toInt))
    } else Seq()

  def initiatePeerReload()(implicit dao: DAO, ec: ExecutionContextExecutor): Unit = {

    tryWithMetric(
      {
        if (dao.peersInfoPath.nonEmpty) {
          dao.peersInfoPath.lines.mkString.x[Seq[PeerMetadata]].foreach { pmd =>
            dao.peerManager ! pmd
          }
        }
      },
      "peerReloading"
    )

    tryWithMetric(
      {
        if (dao.seedsPath.nonEmpty) {
          dao.seedsPath.lines.foreach { line =>
            line.split(":") match {
              case Array(host, port) =>
                Try {
                  dao.peerManager ! HostPort(host, port.toInt)
                } // increment parsing error
              case _ =>
            }

          }
        }
      },
      "readSeedsFile"
    )

    // TODO: Instead wait until peer discovery phase complete
    Thread.sleep(15 * 1000)

    if (dao.peersInfoPath.nonEmpty || dao.seedsPath.nonEmpty) {
      Download.download()
    } else {
      logger.warn("No peers or seeds configured yet. Skipping initial download.")
    }

  }

  def broadcastNodeState()(implicit dao: DAO, ec: ExecutionContext): Unit = {
    broadcast(_.post("status", SetNodeStatus(dao.id, dao.nodeState)))
  }

  def attemptRegisterSelfWithPeer(hp: HostPort)(implicit dao: DAO): Future[Any] = {

    implicit val ec: ExecutionContextExecutor = dao.apiClientExecutionContext
    // if (!dao.peerInfo.exists(_._2.client.hostName == hp.host)) {

    futureTryWithTimeoutMetric(
      {
        logger.info(s"Attempting to register with $hp")

        APIClient(hp.host, hp.port).postSync(
          "register",
          dao.peerRegistrationRequest
        )
      },
      "addPeerWithRegistration"
    )
    //} else {
    // Future.successful(
    //        ()
    //    )
    //    }
  }

  def attemptRegisterPeer(hp: HostPort)(implicit dao: DAO): Future[Response[String]] = {

    implicit val ec: ExecutionContextExecutor = dao.apiClientExecutionContext
    // if (!dao.peerInfo.exists(_._2.client.hostName == hp.host)) {

    wrapFutureWithMetric(
      {
        logger.info(s"Attempting to register with $hp")

        val client =
          APIClient(hp.host, hp.port)(dao.apiClientExecutionContext, dao)

        client
          .getNonBlocking[PeerRegistrationRequest]("registration/request")
          .flatMap { registrationRequest =>
            dao.peerManager ! PendingRegistration(hp.host, registrationRequest)
            client.post("register", dao.peerRegistrationRequest)
          }
          .recover {
            case e: Exception =>
              logger.error("registration request failed", e)
              dao.metrics.incrementMetric("peerGetRegistrationRequestFailed")
              throw e
          }
      },
      "addPeerWithRegistrationSymmetric"
    )

  }

  def peerDiscovery(client: APIClient)(implicit dao: DAO): Unit = {
    client
      .getNonBlocking[Seq[PeerMetadata]]("peers")
      .onComplete {
        case Success(pmd) =>
          pmd.foreach { md =>
            if (dao.id != md.id && validPeerAddition(HostPort(md.host, md.httpPort), dao.peerInfo.unsafeRunSync())) {
              val client =
                APIClient(md.host, md.httpPort)(dao.apiClientExecutionContext, dao)
              client
                .getNonBlocking[PeerRegistrationRequest]("registration/request")
                .onComplete {
                  case Success(registrationRequest) =>
                    dao.peerManager ! PendingRegistration(md.host, registrationRequest)
                    client.post("register", dao.peerRegistrationRequest)
                  case Failure(e) =>
                    dao.metrics.incrementMetric("peerGetRegistrationRequestFailed")
                }(dao.apiClientExecutionContext)
            }
          }
        case Failure(e) =>
          dao.metrics.incrementMetric("peerDiscoveryQueryFailed")

      }(dao.apiClientExecutionContext)
  }

  def validWithLoopbackGuard(host: String)(implicit dao: DAO): Boolean =
    (host != dao.externalHostString && host != "127.0.0.1" && host != "localhost") || !dao.preventLocalhostAsPeer

  def validPeerAddition(hp: HostPort, peerInfo: Map[Id, PeerData])(implicit dao: DAO): Boolean = {
    val hostAlreadyExists = peerInfo.exists {
      case (_, data) =>
        data.client.hostName == hp.host && data.client.apiPort == hp.port
    }
    validWithLoopbackGuard(hp.host) && !hostAlreadyExists
  }

  def broadcast[T](
    func: APIClient => Future[T],
    skipIds: Set[Id] = Set.empty,
    subset: Set[Id] = Set.empty
  )(implicit dao: DAO, ec: ExecutionContext): Future[Map[Id, ValidatedNel[Throwable, T]]] = {

    val peerInfo = dao.peerInfo.unsafeRunSync()
    val selected =
      if (subset.nonEmpty) peerInfo.filterKeys(subset.contains)
      else {
        peerInfo.filterKeys(id => !skipIds.contains(id))
      }

    val (selectedKeys, selectedValues) = selected.toList.unzip

    selectedValues
      .map(_.client)
      .map(func)
      .traverse(_.toValidatedNel)
      .map(values => selectedKeys.zip(values).toMap)
  }
}

case class PeerData(
  peerMetadata: PeerMetadata,
  client: APIClient,
  notification: Seq[PeerNotification] = Seq.empty
)

case class APIBroadcast[T](func: APIClient => Future[T],
                           skipIds: Set[Id] = Set(),
                           peerSubset: Set[Id] = Set())

case class PeerHealthCheck(status: Map[Id, Boolean])

case class PendingRegistration(ip: String, request: PeerRegistrationRequest)

case class Deregistration(ip: String, port: Int, id: Id)

case object GetPeerInfo

case class UpdatePeerInfo(peerData: PeerData)
case class ChangePeerState(id: Id, state: NodeState)

case class PeerNotification(id: Id,
                            state: PeerState,
                            timestamp: LocalDateTime = LocalDateTime.now())
    extends Signable

object PeerState extends Enumeration {
  type PeerState = Value
  val Leave, Join = Value
}
case class UpdatePeerNotifications(notifications: Seq[PeerNotification])

class PeerManager(ipManager: IPManager)(implicit val materialize: ActorMaterializer, dao: DAO)
    extends Actor
    with StrictLogging {

  var peers: Map[Id, PeerData] = Map.empty

  implicit val system: ActorSystem = context.system

  override def receive: Receive = {

    case InternalHeartbeat(round) =>
      if (round % dao.processingConfig.peerHealthCheckInterval == 0) {
        peers.values.foreach { d =>
          d.client
            .getString("health")
            .onComplete {
              case Success(x) if x.isSuccess =>
                dao.metrics.incrementMetric("peerHealthCheckPassed")
              case _ =>
                dao.metrics.incrementMetric("peerHealthCheckFailed")
                self ! ChangePeerState(d.peerMetadata.id, NodeState.Offline)
            }(dao.apiClientExecutionContext)
        }
      }
      if (round % dao.processingConfig.peerDiscoveryInterval == 0) {

        peers.values.foreach { d =>
          PeerManager.peerDiscovery(d.client)
        }

      }

    case hp @ HostPort(host, port) =>
      if (!peers.exists {
            case (_, data) =>
              data.peerMetadata.host == host && data.peerMetadata.httpPort == port
          } &&
          PeerManager.validWithLoopbackGuard(host)) {
        PeerManager.attemptRegisterPeer(hp)
      }

    case UpdatePeerInfo(peerData) =>
      updatePeerInfo(peerData)

    case UpdatePeerNotifications(obsoleteNotifications) =>
      val peerUpdate = obsoleteNotifications
        .flatMap { n =>
          peers.get(n.id).map { p =>
            p.copy(notification = p.notification diff Seq(n))
          }
        }
        .foreach(pd => self ! UpdatePeerInfo(pd))

    case RemovePeerRequest(hp, id) =>
      val updatedPeerInfo = peers.filter {
        case (pid, d) =>
          val badHost = hp.exists {
            case HostPort(host, port) =>
              d.client.hostName == host && d.client.apiPort == port
          }
          val badId = id.contains(pid)
          !badHost && !badId
      }

      if (peers != updatedPeerInfo) {
        dao.metrics.incrementMetric("peerRemoved")
      }

      updateMetricsAndPersistentStore(updatedPeerInfo)

    case SetNodeStatus(id, nodeStatus) =>
      val updated = peers
        .get(id)
        .map { pd =>
          peers + (id -> pd.copy(peerMetadata = pd.peerMetadata.copy(nodeState = nodeStatus)))
        }
        .getOrElse(peers)

      updateMetricsAndPersistentStore(updated)

    case a @ PeerMetadata(host, port, id, ns, time, auxHost, addresses, _, _) =>
      val validHost = (host != dao.externalHostString && host != "127.0.0.1") || !dao.preventLocalhostAsPeer

      if (id != dao.id && validHost) {

        val adjustedHost = if (auxHost.nonEmpty) auxHost else host
        val client =
          APIClient(adjustedHost, port)(dao.apiClientExecutionContext, dao)
        client.id = id

        PeerManager.peerDiscovery(client)

        val peerData = PeerData(a, client)

        updatePeerInfo(peerData)
      }

    case APIBroadcast(func, skipIds, subset) =>
      val replyTo = sender()

      val keys =
        if (subset.nonEmpty) peers.filterKeys(subset.contains)
        else {
          peers.filterKeys(id => !skipIds.contains(id))
        }

      val result = keys.map {
        case (id, data) =>
          id -> func(data.client)
      }

      replyTo ! result

    case GetPeerInfo =>
      val replyTo = sender()
      replyTo ! peers

    case pr @ PendingRegistration(ip, request) =>
      logger.debug(s"Pending Registration request: $pr")

      // TODO: Refactor and add metrics
      // Also should attempt to allow peers to re-register to potentially update their status or ID? Or handle elsewhere.
      val validExternalHost = PeerManager.validWithLoopbackGuard(request.host)
      val hostAlreadyExists = peers.exists {
        case (_, data) =>
          data.client.hostName == request.host && data.client.apiPort == request.port
      }
      val validHost = validExternalHost && !hostAlreadyExists
      val isSelfId = dao.id == request.id

      val badAttempt = isSelfId || !validHost

      if (badAttempt) {
        dao.metrics.incrementMetric("duplicatePeerAdditionAttempt")
      } else {
        implicit val ec: ExecutionContextExecutor =
          dao.apiClientExecutionContext

        val client = APIClient(request.host, request.port)(dao.apiClientExecutionContext, dao)

        val authSignRequest = PeerAuthSignRequest(Random.nextLong())
        val req =
          client.postNonBlocking[SingleHashSignature]("sign", authSignRequest)

        req.failed.foreach { t =>
          logger.warn(s"Sign request to ${request.host}:${request.port} failed.", t)
          dao.metrics.incrementMetric("peerSignatureRequestFailed")
        }

        req.foreach { sig =>
          if (sig.hashSignature.id != request.id) {
            logger.warn(
              s"keys should be the same: ${sig.hashSignature.id} != ${request.id}"
            )
            dao.metrics.incrementMetric("peerKeyMismatch")
          }

          if (!sig.valid) {
            logger.warn(s"Invalid peer signature $request $authSignRequest $sig")
            dao.metrics.incrementMetric("invalidPeerRegistrationSignature")
          }

          dao.metrics.incrementMetric("peerAddedFromRegistrationFlow")

          logger.debug(s"Valid peer signature $request $authSignRequest $sig")

          val stateF = wrapFutureWithMetric(
            client.getNonBlocking[NodeStateInfo]("state"),
            "nodeState"
          )

          stateF.map { s =>
            val state = s.nodeState
            val id = sig.hashSignature.id
            val add =
              PeerMetadata(
                request.host,
                request.port,
                id,
                nodeState = state,
                auxAddresses = s.addresses,
                resourceInfo = request.resourceInfo
              )
            val peerData = PeerData(add, client)
            client.id = id
            self ! UpdatePeerInfo(peerData)

            PeerManager.peerDiscovery(client)
          }

        }
      }

    case Deregistration(ip, port, key) =>
      // Do we need to validate this? Or just remove from knownIPs?
      peers.get(key).foreach { peer =>
        ipManager.removeKnownIP(ip)
        self ! UpdatePeerInfo(
          peer.copy(notification = peer.notification ++ Seq(PeerNotification(key, PeerState.Leave)))
        )
      }
  }

  private def updateMetricsAndPersistentStore(updatedPeerInfo: Map[Id, PeerData]): Unit = {
    dao.metrics.updateMetric(
      "peers",
      updatedPeerInfo
        .map {
          case (idI, clientI) =>
            val addr =
              s"http://${clientI.client.hostName}:${clientI.client.apiPort - 1}"
            s"${idI.short} API: $addr"
        }
        .mkString(" --- ")
    )
    peers = updatedPeerInfo
// TODO: use swayDB when ready
    if (dao.peersInfoPath.exists) {
      dao.peersInfoPath.write(updatedPeerInfo.values.toSeq.map { _.peerMetadata }.json)
    }

  }

  private def updatePeerInfo(peerData: PeerData): Unit = {

    peers = peers + (peerData.client.id -> peerData)

    val ip = peerData.client.hostName
    ipManager.addKnownIP(ip)
    logger.info(s"Added $ip to known peers.")

    updateMetricsAndPersistentStore(peers)
  }

}
