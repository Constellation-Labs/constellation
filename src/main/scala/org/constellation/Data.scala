package org.constellation

import java.io.File
import java.net.InetSocketAddress
import java.security.KeyPair

import org.constellation.primitives.Schema._
import org.constellation.util.{ProductHash, Signed}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.util.Try
import constellation._

class Data {

  @volatile var db : LevelDB = _
  @volatile implicit var keyPair: KeyPair = _

  def publicKeyHash: Int = keyPair.getPublic.hashCode()
  def id : Id = Id(keyPair.getPublic)
  def selfAddress: Address = id.address
  def tmpDirId = new File("tmp", id.medium)

  def updateKeyPair(kp: KeyPair): Unit = {
    keyPair = kp
    Try{db.destroy()}
    db = new LevelDB(new File(tmpDirId, "db"))
  }

  @volatile var memPoolTX: Set[TX] = Set()
  @volatile var validTX: Set[TX] = Set()
  @volatile var validSyncPendingTX: Set[TX] = Set()

  val validUTXO: TrieMap[String, Long] = TrieMap()
  val memPoolUTXO: TrieMap[String, Long] = TrieMap()
  val validSyncPendingUTXO: TrieMap[String, Long] = TrieMap()

  // @volatile var bundlePool: Set[Bundle] = Set()
  // @volatile var superSelfBundle : Option[Bundle] = None

  // TODO: Make this a graph to prevent duplicate storages.
  val txToGossipChains: TrieMap[String, Seq[Gossip[ProductHash]]] = TrieMap()

  // This should be identical to levelDB hashes but I'm putting here as a way to double check
  // Ideally the hash workload should prioritize memory and dump to disk later but can be revisited.
  val addressToTX: TrieMap[String, TX] = TrieMap()

  def selfBalance: Option[Long] = validUTXO.get(id.address.address)

  @volatile var downloadMode: Boolean = true

  @volatile var nodeState: NodeState = PendingDownload

  // @volatile var downloadResponses = Seq[DownloadResponse]()
  @volatile var secretReputation: Map[Id, Double] = Map()
  @volatile var publicReputation: Map[Id, Double] = Map()
  @volatile var deterministicReputation: Map[Id, Double] = Map()


  @volatile var bundles: Set[Bundle] = Set[Bundle]()
  @volatile var bundleBuffer: Set[Bundle] = Set[Bundle]()

  @volatile var externalAddress: InetSocketAddress = _
  @volatile var apiAddress: InetSocketAddress = _
  // @volatile private var peers: Set[InetSocketAddress] = Set.empty[InetSocketAddress]
  var remotes: Set[InetSocketAddress] = Set.empty[InetSocketAddress]
  val peerLookup: mutable.HashMap[InetSocketAddress, Signed[Peer]] = mutable.HashMap[InetSocketAddress, Signed[Peer]]()

  def peerIDLookup: Map[Id, Signed[Peer]] = peerLookup.values.map{ z => z.data.id -> z}.toMap

  def selfPeer: Signed[Peer] = Peer(id, externalAddress, Set(), apiAddress).signed()

  def peerIPs: Set[InetSocketAddress] = peerLookup.values.map(z => z.data.externalAddress).toSet

  def allPeerIPs: Set[InetSocketAddress] = {
    peerLookup.keys ++ peerLookup.values.flatMap(z => z.data.remotes ++ Seq(z.data.externalAddress))
  }.toSet

  def peers: Seq[Signed[Peer]] = peerLookup.values.toSeq.distinct

  var genesisTXHash: String = _

  @volatile var totalNumGossipMessages = 0


}
