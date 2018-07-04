package org.constellation.primitives

import java.net.InetSocketAddress

import akka.actor.ActorRef
import org.constellation.primitives.Schema.{Id, Peer, PeerSyncHeartbeat}
import org.constellation.util.Signed

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

trait PeerInfo {


  val peersAwaitingAuthenticationToNumAttempts: TrieMap[InetSocketAddress, Int] = TrieMap()

  val peerSync: TrieMap[Id, PeerSyncHeartbeat] = TrieMap()

  var p2pActor : ActorRef = _

  val rawPeerLookup: TrieMap[InetSocketAddress, Peer] = TrieMap()

  val signedPeerLookup: mutable.HashMap[InetSocketAddress, Signed[Peer]] = mutable.HashMap[InetSocketAddress, Signed[Peer]]()

  def signedPeerIDLookup: Map[Id, Signed[Peer]] = signedPeerLookup.values.map { z => z.data.id -> z }.toMap

  def peerIPs: Set[InetSocketAddress] = signedPeerLookup.values.map(z => z.data.externalAddress).toSet

  def allPeerIPs: Set[InetSocketAddress] = {
    signedPeerLookup.keys ++ signedPeerLookup.values.flatMap(z => z.data.remotes ++ Seq(z.data.externalAddress))
  }.toSet

  def peers: Seq[Signed[Peer]] = signedPeerLookup.values.toSeq.distinct


}
