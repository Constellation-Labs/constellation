package org.constellation.primitives

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorSystem}
import akka.http.scaladsl.model.RemoteAddress
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.constellation.p2p.{PeerAuthSignRequest, PeerRegistrationRequest}
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.{Id, NodeState}
import org.constellation.util.{APIClient, HashSignature}
import org.constellation.{AddPeerRequest, DAO, HostPort, RemovePeerRequest}

case class SetNodeStatus(id: Id, nodeStatus: NodeState)
import scala.collection.Set
import scala.concurrent.ExecutionContext


case class PeerData(
                     addRequest: AddPeerRequest,
                     client: APIClient,
                     timeAdded: Long = System.currentTimeMillis(),
                     nodeState: NodeState = NodeState.Ready
                   )

case class APIBroadcast[T](func: APIClient => T, skipIds: Set[Id] = Set(), peerSubset: Set[Id] = Set())

case class PeerHealthCheck(status: Map[Id, Boolean])
case class PendingRegistration(ip: String, request: PeerRegistrationRequest)
case class Deregistration(ip: String, port: Int, key: String)

case object GetPeerInfo

class PeerManager(ipManager: IPManager, dao: DAO)(implicit val materialize: ActorMaterializer) extends Actor {

  val logger = Logger(s"PeerManager")

  override def receive: Receive = active(Map.empty)

  implicit val system: ActorSystem = context.system

  def active(peerInfo: Map[Id, PeerData]): Receive = {

    case RemovePeerRequest(hp, id) =>

      val updatedPeerInfo = peerInfo.filter { case (pid, d) =>
        val badHost = hp.exists { case HostPort(host, port) => d.client.hostName == host && d.client.apiPort == port }
        val badId = id.contains(pid)
        !badHost && !badId
      }

      dao.metricsManager ! UpdateMetric(
        "peers",
        updatedPeerInfo.map { case (idI, clientI) =>
          val addr = s"http://${clientI.client.hostName}:${clientI.client.apiPort}"
          s"${idI.short} API: $addr"
        }.mkString(" --- ")
      )
      context become active (updatedPeerInfo)

    case SetNodeStatus(id, nodeStatus) =>

      val updated = peerInfo.get(id).map{
        pd =>
          peerInfo + (id -> pd.copy(nodeState = nodeStatus))
      }.getOrElse(peerInfo)

      context become active(updated)

    case a @ AddPeerRequest(host, udpPort, port, id, ns) =>
      val client =  APIClient(host, port)

      client.id = id
      val updatedPeerInfo = peerInfo + (id -> PeerData(a, client, nodeState = ns))

      val remoteAddr = RemoteAddress(new InetSocketAddress(host, port))
      ipManager.addKnownIP(remoteAddr)

      dao.metricsManager ! UpdateMetric(
        "peers",
        updatedPeerInfo.map { case (idI, clientI) =>
          val addr = s"http://${clientI.client.hostName}:${clientI.client.apiPort}"
          s"${idI.short} API: $addr"
        }.mkString(" --- ")
      )
      // dao.peerInfo = updatedPeerInfo
      context become active(updatedPeerInfo)

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

    case GetPeerInfo => {
      val replyTo = sender()
      replyTo ! peerInfo
    }

    case PendingRegistration(ip, request) =>
      implicit val executionContext: ExecutionContext = system.dispatchers.lookup("api-client-dispatcher")

      val client = APIClient(request.host, request.port)

      val req = client.postNonBlocking[HashSignature]("sign", PeerAuthSignRequest())

      req.failed.foreach { t => logger.warn(s"Sign request to ${request.host}:${request.port} failed.", t)}

      req.foreach { sig =>
        if (sig.b58EncodedPublicKey != request.key) {
          logger.warn(
            s"keys should be the same: ${sig.b58EncodedPublicKey} != ${request.key}"
          )
        }

        // TODO: Not completely sure how to validate this:
        // 1: Presumably we should compare the public key to what we sent earlier. Are there encoding questions?

        // For now -- let's assume successful response

        val remoteAddr = RemoteAddress(
          new InetSocketAddress(request.host, request.port)
        )
        ipManager.addKnownIP(remoteAddr)
        logger.info(s"Added $remoteAddr to known peers.")
      }

    case Deregistration(ip, port, key) =>

    // Do we need to validate this? Or just remove from knownIPs?

  }
}

