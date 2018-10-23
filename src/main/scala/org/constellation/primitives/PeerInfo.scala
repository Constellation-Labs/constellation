package org.constellation.primitives

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.constellation.primitives.Schema.{Id, LocalPeerData, Peer}
import org.constellation.util.{APIClient, Signed}

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContextExecutor

trait PeerInfo {

  @volatile var deadPeers: Seq[InetSocketAddress] = Seq()

  @volatile var bannedIPs: Seq[InetSocketAddress] = Seq.empty[InetSocketAddress]

  val lastPeerRX : TrieMap[Id, Long] = TrieMap()

  val peersAwaitingAuthenticationToNumAttempts: TrieMap[InetSocketAddress, Int] = TrieMap()

  val rawPeerLookup: TrieMap[Id, LocalPeerData] = TrieMap()

  def getOrElseUpdateAPIClient(id: Id)(
    implicit system: ActorSystem, materialize: ActorMaterializer, executionContext: ExecutionContextExecutor
  ): Option[APIClient] = {
    rawPeerLookup.get(id).map { z => Some(z.apiClient) }.getOrElse {
      signedPeerIDLookup.get(id).map { p =>
        val a = p.data.externalHostString
   //     println("Updating api client send to hostname : " + a)
        val client = APIClient(a, p.data.apiAddress.map{_.getPort}.getOrElse(9000))
        rawPeerLookup(id) = LocalPeerData(client)
        client

      }
    }
  }

  val signedPeerLookup: TrieMap[InetSocketAddress, Signed[Peer]] = TrieMap()

  val addressToLastObservedExternalAddress: TrieMap[InetSocketAddress, InetSocketAddress] = TrieMap()

  def signedPeerIDLookup: Map[Id, Signed[Peer]] = signedPeerLookup.values.map { z => z.data.id -> z }.toMap

  def peerIPs: Set[InetSocketAddress] = signedPeerLookup.values.flatMap(z => z.data.externalAddress).toSet

  def peers: Seq[Signed[Peer]] = signedPeerLookup.values.toSeq.distinct

}
