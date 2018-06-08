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

  def transactionData(txHash: String): TransactionQueryResponse = {
    val txOpt = txHashToTX.get(txHash)
    val gossip = txToGossipChains.getOrElse(txHash, Seq())
    TransactionQueryResponse(
      txHash,
      txOpt,
      txHashToTX.contains(txHash),
      txOpt.exists{memPoolTX.contains},
      txOpt.exists{validTX.contains},
      gossip.length,
      gossip.map{_.stackDepth},
      gossip
    )
  }

  val txHashToTX: TrieMap[String, TX] = TrieMap()

  @volatile var sentTX: Seq[TX] = Seq()
  @volatile var memPoolTX: Set[TX] = Set()
  @volatile var validTX: Set[TX] = Set()
  @volatile var validSyncPendingTX: Set[TX] = Set()

  val validLedger: TrieMap[String, Long] = TrieMap()
  val memPoolLedger: TrieMap[String, Long] = TrieMap()
  val validSyncPendingUTXO: TrieMap[String, Long] = TrieMap()

  // @volatile var bundlePool: Set[Bundle] = Set()
  // @volatile var superSelfBundle : Option[Bundle] = None

  // TODO: Make this a graph to prevent duplicate storages.
  val txToGossipChains: TrieMap[String, Seq[Gossip[ProductHash]]] = TrieMap()

  // This should be identical to levelDB hashes but I'm putting here as a way to double check
  // Ideally the hash workload should prioritize memory and dump to disk later but can be revisited.
  val addressToTX: TrieMap[String, TX] = TrieMap()

  def selfBalance: Option[Long] = validLedger.get(id.address.address)

  @volatile var downloadMode: Boolean = true

  @volatile var nodeState: NodeState = PendingDownload

  // @volatile var downloadResponses = Seq[DownloadResponse]()
  @volatile var secretReputation: Map[Id, Double] = Map()
  @volatile var publicReputation: Map[Id, Double] = Map()
  @volatile var deterministicReputation: Map[Id, Double] = Map()


  @volatile var bundles: Seq[Bundle] = Seq[Bundle]()
  @volatile var bundleBuffer: Set[Bundle] = Set[Bundle]()
  val bestBundles: TrieMap[Id, BestBundle] = TrieMap()


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

  var lastSuperBundleHash: String = _

  @volatile var totalNumGossipMessages = 0
  @volatile var totalNumBundleMessages = 0
  @volatile var totalNumBroadcastMessages = 0

  @volatile var bestBundleSelf: Bundle = _

  def acceptTransaction(tx: TX, updatePending: Boolean = true): Unit = {
    validTX += tx
    memPoolTX -= tx
    /*    if (updatePending) {
          tx.updateUTXO(validSyncPendingUTXO)
        }*/
    validSyncPendingTX -= tx
    tx.updateLedger(validLedger)
    if (tx.tx.data.isGenesis) {
      tx.updateLedger(memPoolLedger)
    }
    //    txToGossipChains.remove(tx.hash) // TODO: Remove after a certain period of time instead. Cleanup gossip data.
    val txSeconds = (System.currentTimeMillis() - tx.tx.time) / 1000
    //  logger.debug(s"Accepted TX from $txSeconds seconds ago: ${tx.short} on ${id.short} " +
    //   s"new mempool size: ${memPoolTX.size} valid: ${validTX.size} isGenesis: ${tx.tx.data.isGenesis}")
  }

  var genesisBundle : Bundle = _

}
