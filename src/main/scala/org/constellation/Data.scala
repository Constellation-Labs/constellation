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

  def createGenesis(tx: TX): Unit = {
    if (tx.tx.data.isGenesis) {
      genesisBundle = Bundle(BundleData(Seq(BundleHash(tx.hash), tx)).signed())
      processNewBundleMetadata(genesisBundle)
      validBundles = Seq(genesisBundle)
    }
  }

  @volatile var activeBundles: Seq[Bundle] = Seq[Bundle]()
  @volatile var bundleBuffer: Set[Bundle] = Set[Bundle]()
  val peerSync: TrieMap[Id, PeerSync] = TrieMap()

  var genesisBundle : Bundle = _
  def genesisTXHash: String = genesisBundle.extractTX.head.hash
  @volatile var validBundles : Seq[Bundle] = Seq()
  def lastBundle: Bundle = validBundles.lastOption.getOrElse(genesisBundle)
  def lastBundleHash = BundleHash(validBundles.lastOption.map{_.hash}.getOrElse(Option(genesisBundle).map{_.hash}.getOrElse("")))
  @volatile var bestBundle: Bundle = _
  @volatile var bestBundleBase: Bundle = _
  @volatile var bestBundleCandidateHashes: Set[BundleHash] = Set()

  @volatile var lastSquashed: Option[Bundle] = None

  def jaccard[T](t1: Set[T], t2: Set[T]): Double = {
    t1.intersect(t2).size.toDouble / t1.union(t2).size.toDouble
  }

  implicit class BundleExtData(b: Bundle) {
    def txBelow = bundleHashToTXBelow(b.hash)
    def idBelow = bundleHashToIdsBelow(b.hash)
    def idAbove = bundleHashToIdsAbove(b.hash)
    def bundleScore: Int = {
      txBelow.size + (idBelow.size * 5) + idAbove.size
    }
    def minTime = bundleHashToMinTime(b.hash)
    def maxTime: Long = b.bundleData.time
    def pretty: String = s"hash: ${b.short}, depth: ${b.maxStackDepth}, numTX: ${txBelow.size}, numId: ${idBelow.size}, " +
      s"numIdAbove ${idAbove.size}, score: $bundleScore"
  }

  def prettifyBundle(b: Bundle) = b.pretty



  case class BundleComparison(b1: Bundle, b2: Bundle) {
    def txJaccard: Double = jaccard(bundleHashToTXBelow(b1.hash), bundleHashToTXBelow(b2.hash))
    def idJaccard: Double = jaccard(bundleHashToIdsBelow(b1.hash), bundleHashToIdsBelow(b2.hash))
    def idAboveJaccard: Double = jaccard(bundleHashToIdsAbove(b1.hash), bundleHashToIdsAbove(b2.hash))
  }


  def processNewBundleMetadata(bundle: Bundle, idsAbove: Set[Id] = Set()): Boolean = {

    val hash = bundle.hash
    // Never before seen bundle
    val notPresent = !bundleHashToBundle.contains(hash)
    if (notPresent) {

      activeBundles :+= bundle
      bundleHashToBundle(hash) = bundle
      bundleHashToIdsAbove(hash) = idsAbove

      val txs = bundle.extractTX
      val ids = bundle.extractIds

      bundleHashToIdsBelow(hash) = ids
      bundleHashToTXBelow(hash) = txs.map{_.hash}
      bundleHashToFirstRXTime(hash) = System.currentTimeMillis()

      val sb = bundle.extractSubBundles
      val sbh = sb.map {_.hash}
      bundleHashToBundleHashesBelow(hash) = sbh
      bundleHashToMinTime(hash) = if (sb.isEmpty) bundle.bundleData.time else sb.map{_.bundleData.time}.min

      sb.foreach{s => processNewBundleMetadata(s, idsAbove ++ Set(bundle.bundleData.id))}
      sbh.foreach{ s =>
        if (bundleHashToBundleHashesAbove.contains(s)) bundleHashToBundleHashesAbove(s) += hash
        else bundleHashToBundleHashesAbove(s) = Set(hash)
      }
    }
    notPresent
  }

  @volatile var totalNumGossipMessages = 0
  @volatile var totalNumBundleMessages = 0
  @volatile var totalNumBroadcastMessages = 0

  def acceptTransaction(tx: TX, updatePending: Boolean = true): Unit = {

    if (!validTX.contains(tx)) {
      validTX += tx
      tx.updateLedger(validLedger)
    }

    memPoolTX -= tx

    if (tx.tx.data.isGenesis) {
      tx.updateLedger(memPoolLedger)
    }
    //    txToGossipChains.remove(tx.hash) // TODO: Remove after a certain period of time instead. Cleanup gossip data.
    val txSeconds = (System.currentTimeMillis() - tx.tx.time) / 1000
    //  logger.debug(s"Accepted TX from $txSeconds seconds ago: ${tx.short} on ${id.short} " +
    //   s"new mempool size: ${memPoolTX.size} valid: ${validTX.size} isGenesis: ${tx.tx.data.isGenesis}")
  }


  val bundleHashToIdsAbove: TrieMap[String, Set[Id]] = TrieMap()

  val bundleHashToBundleHashesAbove : TrieMap[String, Set[String]] = TrieMap()

  val bundleHashToBundleHashesBelow : TrieMap[String, Set[String]] = TrieMap()

  val bundleHashToMinTime : TrieMap[String, Long] = TrieMap()

  // val bundlesByStackDepth: TrieMap[Int, Set[String]] = TrieMap()

  val bundleHashToBundle: TrieMap[String, Bundle] = TrieMap()

  val bundleHashToFirstRXTime: TrieMap[String, Long] = TrieMap()

  val bundleHashToIdsBelow: TrieMap[String, Set[Id]] = TrieMap()

  val bundleHashToTXBelow: TrieMap[String, Set[String]] = TrieMap()

  val bundleHashToDepth: TrieMap[String, Int] = TrieMap()



}
