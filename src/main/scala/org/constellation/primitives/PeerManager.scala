package org.constellation.primitives

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorSystem}
import akka.http.scaladsl.model.RemoteAddress
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.constellation.p2p.{PeerAuthSignRequest, PeerRegistrationRequest}
import org.constellation.primitives.Schema.Id
import org.constellation.util.{APIClient, HashSignature}
import org.constellation.{AddPeerRequest, ConstellationNode}

import scala.collection.Set
import scala.concurrent.ExecutionContext

case class PeerData(addRequest: AddPeerRequest, client: APIClient)
case class APIBroadcast[T](func: APIClient => T, skipIds: Set[Id] = Set(), peerSubset: Set[Id] = Set())
case class PeerHealthCheck(status: Map[Id, Boolean])
case class PendingRegistration(ip: String, request: PeerRegistrationRequest)
case class Deregistration(ip: String, port: Int, key: String)

case object GetPeerInfo

class PeerManager(parentNode: ConstellationNode)(implicit val materialize: ActorMaterializer) extends Actor {

  val logger = Logger(s"PeerManager")

  override def receive = active(Map.empty)

  implicit val system: ActorSystem = context.system

  def active(peerInfo: Map[Id, PeerData]): Receive = {



    case a @ AddPeerRequest(host, udpPort, port, id) =>
      val client = APIClient(host, port)

      client.id = id
      context become active(peerInfo + (id -> PeerData(a, client)))

    case APIBroadcast(func, skipIds, subset) =>

      val keys = if (subset.nonEmpty) peerInfo.filterKeys(subset.contains) else {
        peerInfo.filterKeys(id => !skipIds.contains(id))
      }

      val result = keys.map {
        case (id, data) =>
          id -> func(data.client)
      }

      sender() ! result

    case GetPeerInfo => {
      sender() ! peerInfo
    }

    case PendingRegistration(ip, request) =>
      implicit val executionContext: ExecutionContext = system.dispatchers.lookup("api-client-dispatcher")

      val client = APIClient(request.host, request.port)
      val req = PeerAuthSignRequest()

      // TODO: Either need to move this somewhere else, or do non-blocking
      // Definitely don't want to be blocking the entire actor while we wait...

      client.postNonBlocking[HashSignature]("/sign", req).foreach { sig =>
        if (sig.b58EncodedPublicKey != request.key) {
          logger.warn(s"keys should be the same: ${sig.b58EncodedPublicKey} != ${request.key}")
        }

        // TODO: Not completely sure how to validate this:
        // 1: Presumably we should compare the public key to what we sent earlier. Are there encoding questions?

        // For now -- let's assume successful response

        val remoteAddr = RemoteAddress(new InetSocketAddress(request.host, request.port))
        parentNode.IPManager.addKnownIp(remoteAddr)
      }



    case Deregistration(ip, port, key) =>

      // Do we need to validate this? Or just remove from knownIPs?

  }
}

