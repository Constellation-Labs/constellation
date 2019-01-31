package org.constellation.primitives

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorSystem}
import akka.http.scaladsl.model.RemoteAddress
import akka.stream.ActorMaterializer
import com.softwaremill.sttp.Response
import com.typesafe.scalalogging.Logger
import constellation.futureTryWithTimeoutMetric
import org.constellation.p2p.{Download, PeerAuthSignRequest, PeerRegistrationRequest}
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.{Id, InternalHeartbeat}
import org.constellation.util._
import org.constellation.{DAO, HostPort, PeerMetadata, RemovePeerRequest}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

case class SetNodeStatus(id: Id, nodeStatus: NodeState)
import constellation._

import scala.collection.Set
import scala.util.{Failure, Success}

object PeerManager {

  def initiatePeerReload()(implicit dao: DAO,
                           ec: ExecutionContextExecutor): Unit = {

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
      logger.warn(
        "No peers or seeds configured yet. Skipping initial download.")
    }

  }

  def broadcastNodeState()(implicit dao: DAO): Unit = {
    dao.peerManager ! APIBroadcast(
      _.post("status", SetNodeStatus(dao.id, dao.nodeState)))
  }

  val logger = Logger(s"PeerManagerObj")

  def attemptRegisterSelfWithPeer(hp: HostPort)(
      implicit dao: DAO): Future[Any] = {

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

  def attemptRegisterPeer(hp: HostPort)(
      implicit dao: DAO): Future[Response[String]] = {

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
            case e: Throwable =>
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
            if (dao.id != md.id && validPeerAddition(HostPort(md.host,
                                                              md.httpPort),
                                                     dao.peerInfo)) {
              val client =
                APIClient(md.host, md.httpPort)(dao.apiClientExecutionContext,
                                                dao)
              client
                .getNonBlocking[PeerRegistrationRequest]("registration/request")
                .onComplete {
                  case Success(registrationRequest) =>
                    dao.peerManager ! PendingRegistration(md.host,
                                                          registrationRequest)
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
    (host != dao.externalHostString && host != "127.0.0.1") || !dao.preventLocalhostAsPeer

  def validPeerAddition(hp: HostPort, peerInfo: Map[Id, PeerData])(
      implicit dao: DAO): Boolean = {
    val hostAlreadyExists = peerInfo.exists {
      case (_, data) =>
        data.client.hostName == hp.host && data.client.apiPort == hp.port
    }
    validWithLoopbackGuard(hp.host) && !hostAlreadyExists
  }

}

case class PeerData(
    peerMetadata: PeerMetadata,
    client: APIClient
)

case class APIBroadcast[T](func: APIClient => T,
                           skipIds: Set[Id] = Set(),
                           peerSubset: Set[Id] = Set())

case class PeerHealthCheck(status: Map[Id, Boolean])
case class PendingRegistration(ip: String, request: PeerRegistrationRequest)
case class Deregistration(ip: String, port: Int, id: Id)

case object GetPeerInfo

case class UpdatePeerInfo(peerData: PeerData)

class PeerManager(ipManager: IPManager)(
    implicit val materialize: ActorMaterializer,
    dao: DAO)
    extends Actor {

  val logger = Logger(s"PeerManager")

  override def receive: Receive = active(Map.empty)

  implicit val system: ActorSystem = context.system

  private def updateMetricsAndDAO(updatedPeerInfo: Map[Id, PeerData]): Unit = {
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
    dao.peerInfo = updatedPeerInfo

    dao.peersInfoPath.write(
      updatedPeerInfo.values.toSeq.map { _.peerMetadata }.json)

    context become active(updatedPeerInfo)
  }

  private def updatePeerInfo(peerInfo: Map[Id, PeerData],
                             peerData: PeerData) = {
    val updatedPeerInfo = peerInfo + (peerData.client.id -> peerData)

    val remoteAddr = RemoteAddress(
      new InetSocketAddress(peerData.client.hostName, peerData.client.apiPort))
    ipManager.addKnownIP(remoteAddr)
    logger.info(s"Added $remoteAddr to known peers.")

    updateMetricsAndDAO(updatedPeerInfo)
    updatedPeerInfo
  }

  def active(peerInfo: Map[Id, PeerData]): Receive = {

    case InternalHeartbeat(round) =>
      if (round % dao.processingConfig.peerHealthCheckInterval == 0) {
        peerInfo.values.foreach { d =>
          d.client
            .get("health")
            .onComplete {
              case Success(x) if x.isSuccess =>
                dao.metrics.incrementMetric("peerHealthCheckPassed")
              case _ =>
                dao.metrics.incrementMetric("peerHealthCheckFailed")
                self ! RemovePeerRequest(
                  Some(HostPort(d.peerMetadata.host, d.peerMetadata.httpPort)))
            }(dao.apiClientExecutionContext)
        }
      }

      if (round % dao.processingConfig.peerDiscoveryInterval == 0) {

        peerInfo.values.foreach { d =>
          PeerManager.peerDiscovery(d.client)
        }

      }

    case hp @ HostPort(host, port) =>
      if (!peerInfo.exists {
            case (_, data) =>
              data.peerMetadata.host == host && data.peerMetadata.httpPort == port
          } &&
          PeerManager.validWithLoopbackGuard(host)) {
        PeerManager.attemptRegisterPeer(hp)
      }

    case UpdatePeerInfo(peerData) =>
      updatePeerInfo(peerInfo, peerData)

    case RemovePeerRequest(hp, id) =>
      val updatedPeerInfo = peerInfo.filter {
        case (pid, d) =>
          val badHost = hp.exists {
            case HostPort(host, port) =>
              d.client.hostName == host && d.client.apiPort == port
          }
          val badId = id.contains(pid)
          !badHost && !badId
      }

      if (peerInfo != updatedPeerInfo) {
        dao.metrics.incrementMetric("peerRemoved")
      }

      updateMetricsAndDAO(updatedPeerInfo)

    case SetNodeStatus(id, nodeStatus) =>
      val updated = peerInfo
        .get(id)
        .map { pd =>
          peerInfo + (id -> pd.copy(
            peerMetadata = pd.peerMetadata.copy(nodeState = nodeStatus)))
        }
        .getOrElse(peerInfo)

      updateMetricsAndDAO(updated)

    case a @ PeerMetadata(host, udpPort, port, id, ns, time, auxHost) =>
      val validHost = (host != dao.externalHostString && host != "127.0.0.1") || !dao.preventLocalhostAsPeer

      if (id != dao.id && validHost) {

        val adjustedHost = if (auxHost.nonEmpty) auxHost else host
        val client =
          APIClient(adjustedHost, port)(dao.apiClientExecutionContext, dao)
        client.id = id

        PeerManager.peerDiscovery(client)

        val peerData = PeerData(a, client)

        updatePeerInfo(peerInfo, peerData)
      }

    case APIBroadcast(func, skipIds, subset) =>
      val replyTo = sender()

      val keys =
        if (subset.nonEmpty) peerInfo.filterKeys(subset.contains)
        else {
          peerInfo.filterKeys(id => !skipIds.contains(id))
        }

      val result = keys.map {
        case (id, data) =>
          id -> func(data.client)
      }

      replyTo ! result

    case GetPeerInfo =>
      val replyTo = sender()
      replyTo ! peerInfo

    case pr @ PendingRegistration(ip, request) =>
      logger.debug(s"Pending Registration request: $pr")

      // TODO: Refactor and add metrics
      // Also should attempt to allow peers to re-register to potentially update their status or ID? Or handle elsewhere.
      val validExternalHost = PeerManager.validWithLoopbackGuard(request.host)
      val hostAlreadyExists = peerInfo.exists {
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

        val client = APIClient(request.host, request.port)(
          dao.apiClientExecutionContext,
          dao)

        val authSignRequest = PeerAuthSignRequest(Random.nextLong())
        val req =
          client.postNonBlocking[SingleHashSignature]("sign", authSignRequest)

        req.failed.foreach { t =>
          logger.warn(
            s"Sign request to ${request.host}:${request.port} failed.",
            t)
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
            logger.warn(
              s"Invalid peer signature $request $authSignRequest $sig")
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
              PeerMetadata(request.host,
                           16180,
                           request.port,
                           id,
                           nodeState = state)
            val peerData = PeerData(add, client)
            client.id = id
            self ! UpdatePeerInfo(peerData)

            PeerManager.peerDiscovery(client)
          }

        }
      }

    case Deregistration(ip, port, key) =>
    // Do we need to validate this? Or just remove from knownIPs?

  }
}
