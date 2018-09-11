package org.constellation.primitives

import akka.actor.Actor
import akka.stream.ActorMaterializer
import org.constellation.AddPeerRequest
import org.constellation.p2p.PeerRegistrationRequest
import org.constellation.primitives.Schema.Id
import org.constellation.util.APIClient

case class PeerData(addRequest: AddPeerRequest, client: APIClient)
case class APIBroadcast[T](func: APIClient => T, skipIds: Set[Id] = Set(), peerSubset: Set[Id] = Set())
case class PeerHealthCheck(status: Map[Id, Boolean])
case class PendingRegistration(ip: String, request: PeerRegistrationRequest)

case object GetPeerInfo

class PeerManager()(implicit val materialize: ActorMaterializer) extends Actor {

  override def receive = active(Map.empty)

  def active(peerInfo: Map[Id, PeerData]): Receive = {

    case a @ AddPeerRequest(host, udpPort, port, id) =>
      val client = new APIClient()(context.system, materialize).setConnection(host, port)

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

    case GetPeerInfo => sender() ! peerInfo

    case PendingRegistration(ip, request) =>
      // TODO: Make request to sign endpoint.

  }
}

