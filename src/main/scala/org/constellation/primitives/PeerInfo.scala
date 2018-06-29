package org.constellation.primitives

import java.net.InetSocketAddress

import akka.actor.ActorRef
import org.constellation.primitives.Schema.{Id, Peer, PeerSyncHeartbeat}
import org.constellation.util.Signed

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

trait PeerInfo {

  val peerSync: TrieMap[Id, PeerSyncHeartbeat] = TrieMap()

  var p2pActor : ActorRef = _

  val peerLookup: mutable.HashMap[InetSocketAddress, Signed[Peer]] = mutable.HashMap[InetSocketAddress, Signed[Peer]]()

  def peerIDLookup: Map[Id, Signed[Peer]] = peerLookup.values.map { z => z.data.id -> z }.toMap

  def peerIPs: Set[InetSocketAddress] = peerLookup.values.map(z => z.data.externalAddress).toSet

  def allPeerIPs: Set[InetSocketAddress] = {
    peerLookup.keys ++ peerLookup.values.flatMap(z => z.data.remotes ++ Seq(z.data.externalAddress))
  }.toSet

  def peers: Seq[Signed[Peer]] = peerLookup.values.toSeq.distinct


}
