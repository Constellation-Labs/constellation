package org.constellation.primitives

import akka.actor.Actor
import akka.stream.ActorMaterializer
import org.constellation.{AddPeerRequest, DAO, HostPort, RemovePeerRequest}
import org.constellation.primitives.Schema.{Id, NodeState}
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.util.APIClient


case class SetNodeStatus(id: Id, nodeStatus: NodeState)


case class PeerData(
                     addRequest: AddPeerRequest,
                     client: APIClient,
                     timeAdded: Long = System.currentTimeMillis(),
                     nodeState: NodeState = NodeState.Ready
                   )

case class APIBroadcast[T](func: APIClient => T, skipIds: Set[Id] = Set(), peerSubset: Set[Id] = Set())

case class PeerHealthCheck(status: Map[Id, Boolean])

case object GetPeerInfo

class PeerManager(dao: DAO)(implicit val materialize: ActorMaterializer) extends Actor {

  override def receive: Receive = active(Map.empty)

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
      val client = new APIClient()(context.system, materialize).setConnection(host, port)

      client.id = id
      val updatedPeerInfo = peerInfo + (id -> PeerData(a, client, nodeState = ns))

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

  }
}

