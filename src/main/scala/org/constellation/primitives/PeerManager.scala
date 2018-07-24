package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import akka.stream.ActorMaterializer
import org.constellation.AddPeerRequest
import org.constellation.primitives.Schema.{Id, LocalPeerData}
import org.constellation.util.APIClient

import scala.collection.mutable
import scala.concurrent.Await
import scala.util.Try

case class PeerData(addRequest: AddPeerRequest, client: APIClient)
case class APIBroadcast[T](func: APIClient => T, skipIds: Set[Id] = Set(), peerSubset: Set[Id] = Set())
case class PeerHealthCheck(status: Map[Id, Boolean])
import scala.concurrent.duration._

case object GetPeerInfo

class PeerManager()(implicit val materialize: ActorMaterializer) extends Actor {

  private val peerInfo = mutable.HashMap[Id, PeerData]()

  override def receive: Receive = {

    case a @ AddPeerRequest(host, udpPort, port, id) =>
      val client = new APIClient()(context.system, context.dispatcher, materialize).setConnection(host, port)
      client.id = id
      peerInfo(id) = PeerData(a, client)

    case APIBroadcast(func, skipIds, subset) =>

      val keys = if (subset.nonEmpty) peerInfo.filterKeys(subset.contains) else {
        peerInfo.filterKeys(id => !skipIds.contains(id))
      }

      val result = keys.map {
        case (id, data) =>
          id -> func(data.client)
      }
      sender() ! result

    case GetPeerInfo => sender() ! peerInfo.toMap

  }
}

