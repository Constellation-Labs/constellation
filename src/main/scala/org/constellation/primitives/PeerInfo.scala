package org.constellation.primitives

import java.net.InetSocketAddress

import akka.actor.ActorRef
import org.constellation.primitives.Schema.{Id, LocalPeerObservation, Peer, PeerSyncHeartbeat}
import org.constellation.util.Signed

import scala.collection.concurrent.TrieMap

trait PeerInfo {

  @volatile var deadPeers: Seq[InetSocketAddress] = Seq()

  val lastPeerRX : TrieMap[Id, Long] = TrieMap()

  val peersAwaitingAuthenticationToNumAttempts: TrieMap[InetSocketAddress, Int] = TrieMap()

  val peerSync: TrieMap[Id, PeerSyncHeartbeat] = TrieMap()

  var p2pActor : Option[ActorRef] = None

  val rawPeerLookup: TrieMap[InetSocketAddress, LocalPeerObservation] = TrieMap()

  val signedPeerLookup: TrieMap[InetSocketAddress, Signed[Peer]] = TrieMap()

  val addressToLastObservedExternalAddress: TrieMap[InetSocketAddress, InetSocketAddress] = TrieMap()

  def signedPeerIDLookup: Map[Id, Signed[Peer]] = signedPeerLookup.values.map { z => z.data.id -> z }.toMap

  def peerIPs: Set[InetSocketAddress] = signedPeerLookup.values.flatMap(z => z.data.externalAddress).toSet

  def peers: Seq[Signed[Peer]] = signedPeerLookup.values.toSeq.distinct

}
