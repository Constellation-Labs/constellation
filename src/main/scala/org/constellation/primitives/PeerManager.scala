package org.constellation.primitives

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorSystem}
import akka.http.scaladsl.model.RemoteAddress
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import constellation.futureTryWithTimeoutMetric
import org.constellation.p2p.{Download, PeerAuthSignRequest, PeerRegistrationRequest}
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.{Id, NodeState}
import org.constellation.util._
import org.constellation.{DAO, HostPort, PeerMetadata, RemovePeerRequest}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

case class SetNodeStatus(id: Id, nodeStatus: NodeState)
import scala.collection.Set
import scala.concurrent.ExecutionContext

import constellation._
import better.files._
import scala.util.{Success, Failure}

object PeerManager {

  def initiatePeerReload()(implicit dao: DAO, ec: ExecutionContextExecutor): Unit = {

    tryWithMetric(
      {
        dao.peersInfoPath.lines.mkString.x[Seq[PeerMetadata]].foreach{
          pmd =>
            dao.peerManager ! pmd
        }
      },
      "peerReloading"
    )

    // TODO: Instead wait until peer discovery phase complete
    Thread.sleep(15*1000)
    Download.download()


  }

  def broadcastNodeState()(implicit dao: DAO): Unit = {
    dao.peerManager ! APIBroadcast(_.post("status", SetNodeStatus(dao.id, dao.nodeState)))
  }



  val logger = Logger(s"PeerManagerObj")

  def attemptRegisterPeer(hp: HostPort)(implicit dao: DAO): Future[Any] = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext
    // if (!dao.peerInfo.exists(_._2.client.hostName == hp.host)) {

    futureTryWithTimeoutMetric({
      logger.info(s"Attempting to register with $hp")

      new APIClient(hp.host, hp.port).postSync(
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

}

case class PeerData(
                     peerMetadata: PeerMetadata,
                     client: APIClient
                   )

case class APIBroadcast[T](func: APIClient => T, skipIds: Set[Id] = Set(), peerSubset: Set[Id] = Set())

case class PeerHealthCheck(status: Map[Id, Boolean])
case class PendingRegistration(ip: String, request: PeerRegistrationRequest)
case class Deregistration(ip: String, port: Int, key: String)

case object GetPeerInfo

case class UpdatePeerInfo(peerData: PeerData)

class PeerManager(ipManager: IPManager)(implicit val materialize: ActorMaterializer, dao: DAO) extends Actor {

  val logger = Logger(s"PeerManager")

  override def receive: Receive = active(Map.empty)

  implicit val system: ActorSystem = context.system


  private def updateMetricsAndDAO(updatedPeerInfo: Map[Id, PeerData]): Unit = {
    dao.metricsManager ! UpdateMetric(
      "peers",
      updatedPeerInfo.map { case (idI, clientI) =>
        val addr = s"http://${clientI.client.hostName}:${clientI.client.apiPort - 1}"
        s"${idI.short} API: $addr"
      }.mkString(" --- ")
    )
    dao.peerInfo = updatedPeerInfo

    dao.peersInfoPath.write(updatedPeerInfo.values.toSeq.map{_.peerMetadata}.json)

    context become active(updatedPeerInfo)
  }

  private def updatePeerInfo(peerInfo: Map[Id, PeerData], peerData: PeerData): Unit = {
    val updatedPeerInfo = peerInfo + (peerData.client.id -> peerData)

    val remoteAddr = RemoteAddress(new InetSocketAddress(peerData.client.hostName, peerData.client.apiPort))
    ipManager.addKnownIP(remoteAddr)
    logger.info(s"Added $remoteAddr to known peers.")

    updateMetricsAndDAO(updatedPeerInfo)
  }

  def active(peerInfo: Map[Id, PeerData]): Receive = {

    case UpdatePeerInfo(peerData) =>

      updatePeerInfo(peerInfo, peerData)

    case RemovePeerRequest(hp, id) =>

      val updatedPeerInfo = peerInfo.filter { case (pid, d) =>
        val badHost = hp.exists { case HostPort(host, port) => d.client.hostName == host && d.client.apiPort == port }
        val badId = id.contains(pid)
        !badHost && !badId
      }

      updateMetricsAndDAO(updatedPeerInfo)

    case SetNodeStatus(id, nodeStatus) =>

      val updated = peerInfo.get(id).map{
        pd =>
          peerInfo + (id -> pd.copy(peerMetadata = pd.peerMetadata.copy(nodeState = nodeStatus)))
      }.getOrElse(peerInfo)

      updateMetricsAndDAO(updated)

    case a @ PeerMetadata(host, udpPort, port, id, ns, time, auxHost) =>

      val validHost = (host != dao.externalHostString && host != "127.0.0.1") || !dao.preventLocalhostAsPeer

      if (id != dao.id && validHost) {

        val adjustedHost = if (auxHost.nonEmpty) auxHost else host
        val client = APIClient(adjustedHost, port)(dao.edgeExecutionContext, dao)
        client.id = id

        client.getNonBlocking[Seq[PeerMetadata]]("peers").onComplete {

          case Success(pmd) =>
            pmd.foreach {
              md =>
                if (!dao.peerInfo.exists(_._2.peerMetadata.host == md.host) && md.host != dao.externalHostString &&
                md.host != "127.0.0.1" && dao.id != md.id) {
                  new APIClient(md.host, md.httpPort)(dao.edgeExecutionContext, dao)
                    .getNonBlocking[PeerRegistrationRequest]("registration/request").onComplete {
                    case Success(registrationRequest) =>
                      self ! PendingRegistration(md.host, registrationRequest)
                    case Failure(e) =>
                      dao.metricsManager ! IncrementMetric("peerGetRegistrationRequestFailed")
                  }(dao.edgeExecutionContext)
                }

            }

          case Failure(e) =>
            dao.metricsManager ! IncrementMetric("peerDiscoveryQueryFailed")

        }(dao.edgeExecutionContext)


        val peerData = PeerData(a, client)

        updatePeerInfo(peerInfo, peerData)
      }

    case APIBroadcast(func, skipIds, subset) =>
      val replyTo = sender()

      val keys = if (subset.nonEmpty) peerInfo.filterKeys(subset.contains) else {
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

      logger.info(s"Pending Registration request: $pr")

      val validExternalHost = request.host != dao.externalHostString && request.host != "127.0.0.1"
      val hostAlreadyExists = peerInfo.exists(_._2.client.hostName == ip)
      val validHost = (validExternalHost && !hostAlreadyExists) || !dao.preventLocalhostAsPeer
      val isSelfId = dao.id == request.id

      val badAttempt = isSelfId || !validHost

      if (badAttempt) {
        dao.metricsManager ! IncrementMetric("duplicatePeerAdditionAttempt")
      } else {
        // implicit val executionContext: ExecutionContext = system.dispatchers.lookup("api-client-dispatcher")
        implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

        val client = APIClient(request.host, request.port)(dao.edgeExecutionContext, dao)

        val authSignRequest = PeerAuthSignRequest(Random.nextLong())
        val req = client.postNonBlocking[SingleHashSignature]("sign", authSignRequest)

        req.failed.foreach { t =>
          logger.warn(s"Sign request to ${request.host}:${request.port} failed.", t)
          dao.metricsManager ! IncrementMetric("peerSignatureRequestFailed")
        }

        req.foreach { sig =>
          if (sig.hashSignature.b58EncodedPublicKey != request.key) {
            logger.warn(
              s"keys should be the same: ${sig.hashSignature.b58EncodedPublicKey} != ${request.key}"
            )
            dao.metricsManager ! IncrementMetric("peerKeyMismatch")
          }

          if (!sig.valid) {
            logger.info(s"Invalid peer signature $request $authSignRequest $sig")
            dao.metricsManager ! IncrementMetric("invalidPeerRegistrationSignature")
          }

          dao.metricsManager ! IncrementMetric("peerAddedFromRegistrationFlow")

          logger.info(s"Valid peer signature $request $authSignRequest $sig")

          val state = client.getBlocking[NodeStateInfo]("state").nodeState

          val id = Id(EncodedPublicKey(sig.hashSignature.b58EncodedPublicKey))
          val add = PeerMetadata(request.host, 16180, request.port, id, nodeState = state)
          val peerData = PeerData(add, client)
          client.id = id
          self ! UpdatePeerInfo(peerData)


        }
      }

    case Deregistration(ip, port, key) =>

    // Do we need to validate this? Or just remove from knownIPs?

  }
}

