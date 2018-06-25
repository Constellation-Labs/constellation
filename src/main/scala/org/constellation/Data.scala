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
import org.constellation.consensus.Consensus.{CC, RoundHash}

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
  @volatile var linearMemPoolTX: Set[TX] = Set()

  @volatile var validTX: Set[TX] = Set()
  @volatile var linearValidTX: Set[TX] = Set()

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

  val checkpointsInProgress: TrieMap[RoundHash[_ <: CC], Boolean] = TrieMap()

  @volatile var nodeState: NodeState = PendingDownload

  // @volatile var downloadResponses = Seq[DownloadResponse]()
  @volatile var secretReputation: Map[Id, Double] = Map()
  @volatile var publicReputation: Map[Id, Double] = Map()
  @volatile var normalizedDeterministicReputation: Map[Id, Double] = Map()
  @volatile var deterministicReputation: Map[Id, Int] = Map()


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
      genesisBundle = Bundle(BundleData(Seq(ParentBundleHash(tx.hash), tx)).signed())
      processNewBundleMetadata(genesisBundle, genesisBundle.extractTX, true)
      validBundles = Seq(genesisBundle)
    }
  }

  // @volatile var allBundles: Set[Bundle] = Set[Bundle]()

  @volatile var linearCheckpointBundles: Set[Bundle] = Set[Bundle]()
  @volatile var activeDAGBundles: Seq[Bundle] = Seq[Bundle]()
  val peerSync: TrieMap[Id, PeerSyncHeartbeat] = TrieMap()

  var genesisBundle : Bundle = _
  def genesisTXHash: String = genesisBundle.extractTX.head.hash
  @volatile var validBundles : Seq[Bundle] = Seq()
  def lastBundle: Bundle = validBundles.lastOption.getOrElse(genesisBundle)
  def lastBundleHash = ParentBundleHash(validBundles.lastOption.map{_.hash}.getOrElse(Option(genesisBundle).map{_.hash}.getOrElse("")))
  @volatile var bestBundle: Bundle = _
  @volatile var bestBundleBase: Bundle = _
  @volatile var bestBundleCandidateHashes: Set[BundleHash] = Set()
  @volatile var lastSquashed: Option[Bundle] = None


  @volatile var bundlePendingTX : Set[TX] = Set()

  val unknownParentBundleHashes: TrieMap[String, PeerSyncHeartbeat] = TrieMap()


  def jaccard[T](t1: Set[T], t2: Set[T]): Double = {
    t1.intersect(t2).size.toDouble / t1.union(t2).size.toDouble
  }

  implicit class BundleExtData(b: Bundle) {
    def txBelow = bundleHashToTXBelow(b.hash)
    def idBelow = bundleHashToIdsBelow(b.hash)
    // def idAbove = bundleHashToIdsAbove(b.hash)
    def repScore: Double = idBelow.toSeq.map { id => normalizedDeterministicReputation.getOrElse(id, 0.1)}.sum
    def bundleScore: Double = {
      b.maxStackDepth * 100 +
        txBelow.size +
        repScore * 10
    }
    def minTime: Long = meta.rxTime
    def maxTime: Long = b.bundleData.time
    def pretty: String = s"hash: ${b.short}, depth: ${b.maxStackDepth}, numTX: ${txBelow.size}, numId: ${idBelow.size}, " +
      s"score: $bundleScore, totalScore: ${meta.totalScore}, height: ${meta.height}" // numIdAbove ${idAbove.size},
    def meta = bundleHashToBundleMetaData(b.hash)
  }

  def prettifyBundle(b: Bundle): String = b.pretty

  def calculateReputationsFromScratch(): Unit = {
    val sum = mutable.HashMap[Id, Int]()
    validBundles.foreach{ v =>
      v.idBelow.foreach{id =>
        if (!sum.contains(id)) sum(id) = 1
        else sum(id) = sum(id) + 1
      }
    }
    val total = sum.values.sum
    val map = sum.toMap
    deterministicReputation = map
    val normalized = map.map{
      case (id, r) =>
        id -> r.toDouble / total.toDouble
    }
    normalizedDeterministicReputation = normalized
  }

  case class BundleComparison(b1: Bundle, b2: Bundle) {
    def txJaccard: Double = jaccard(bundleHashToTXBelow(b1.hash), bundleHashToTXBelow(b2.hash))
    def idJaccard: Double = jaccard(bundleHashToIdsBelow(b1.hash), bundleHashToIdsBelow(b2.hash))
    def idAboveJaccard: Double = jaccard(bundleHashToIdsAbove(b1.hash), bundleHashToIdsAbove(b2.hash))
  }

  def peerSyncInfo() = {
    peerSync.map{
      case (id, ps) =>
        ps.validBundleHashes
    }
  }

  // Only call this if the parent chain is known and valid and this is already validated.
  def processNewBundleMetadata(
                                bundle: Bundle,
                                validatedTXs: Set[TX],
                                isGenesis: Boolean = false,
                                setActive: Boolean = true
                                //  idsAbove: Set[Id] = Set()
                              ): Unit = {

    val rxTime = System.currentTimeMillis()

    val hash = bundle.hash

    if (setActive) {
      activeDAGBundles :+= bundle
    }

    bundleHashToBundle(hash) = bundle
    //  bundleHashToIdsAbove(hash) = idsAbove // Not used currently. Will be in future.

    val txs = validatedTXs
    val ids = bundle.extractIds

    bundleHashToIdsBelow(hash) = ids
    bundleHashToTXBelow(hash) = txs.map{_.hash}

    if (!isGenesis) {
      val parentHash = bundle.extractParentBundleHash.hash
      val parentInfo = bundleHashToBundleMetaData(parentHash)

      bundleHashToBundleMetaData(hash) = BundleMetaData(
        parentInfo.height + 1, txs.size, ids.size, bundle.bundleScore, bundle.bundleScore + parentInfo.totalScore, parentHash, rxTime
      )
    } else {
      bundleHashToBundleMetaData(hash) = BundleMetaData(
        0, 1, 1, 1, 1, txs.head.hash, rxTime
      )
    }
  }

  @volatile var totalNumGossipMessages = 0
  @volatile var totalNumBundleMessages = 0
  @volatile var totalNumBundleHashRequests = 0
  @volatile var totalNumInvalidBundles = 0
  @volatile var totalNumNewBundleAdditions = 0
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

  val bundleHashToBundleMetaData: TrieMap[String, BundleMetaData] = TrieMap()

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


  @volatile var lastCheckpointBundle: Option[Bundle] = None

}
