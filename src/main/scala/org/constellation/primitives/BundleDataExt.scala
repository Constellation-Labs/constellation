package org.constellation.primitives

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.util.Timeout
import cats.Monoid
import com.softwaremill.macmemo.memoize
import constellation._
import org.constellation.LevelDB.{DBDelete, DBGet, DBPut}
import org.constellation.consensus.Consensus.{CC, RoundHash}
import org.constellation.primitives.Schema._

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._


trait BundleDataExt extends Reputation with MetricsExt with TransactionExt {

  // @volatile var db: LevelDB

  var confirmWindow : Int

  var genesisBundle : Option[Bundle] = None
  def genesisTXHash: Option[String] = genesisBundle.map{_.extractTX.head.hash}

  @volatile var last100ValidBundleMetaData : Seq[Sheaf] = Seq()
  def lastValidBundle: Bundle = last100ValidBundleMetaData.last.bundle
  def lastValidBundleHash = ParentBundleHash(lastValidBundle.hash)

  @volatile var maxBundleMetaData: Option[Sheaf] = None
  def maxBundle: Option[Bundle] = maxBundleMetaData.map{_.bundle}

  @volatile var syncPendingBundleHashes: Set[String] = Set()
  @volatile var syncPendingTXHashes: Set[String] = Set()

  val activeDAGManager = new ActiveDAGManager()

 // @volatile var activeDAGBundles: Seq[Sheaf] = Seq()
  def activeDAGBundles: Seq[Sheaf] = activeDAGManager.activeSheafs

  @volatile var linearCheckpointBundles: Set[Bundle] = Set[Bundle]()
  @volatile var lastCheckpointBundle: Option[Bundle] = None
  val checkpointsInProgress: TrieMap[RoundHash[_ <: CC], Boolean] = TrieMap()

  @volatile var txInMaxBundleNotInValidation: Set[String] = Set()

  val bundleToSheaf : TrieMap[String, Sheaf] = TrieMap()

  def deleteBundle(hash: String, dbDelete: Boolean = true): Unit = {
    if (bundleToSheaf.contains(hash)) {
      numDeletedBundles += 1
      bundleToSheaf.remove(hash)
    }
    if (dbDelete) {
      dbActor.foreach {
        _ ! DBDelete(hash)
      }
    }
  }

  @volatile var lastCleanupHeight = 0

  def lookupBundleDB(hash: String) : Option[Sheaf] = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    dbActor.flatMap{ d => (d ? DBGet(hash)).mapTo[Option[Sheaf]].getOpt(t=5).flatten }
  }

  def lookupBundleDBFallbackBlocking(hash: String): Option[Sheaf] = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    def dbQuery = {
      dbActor.flatMap{ d => (d ? DBGet(hash)).mapTo[Option[Sheaf]].getOpt(t=5).flatten }
    }
    val res = bundleToSheaf.get(hash)
    if (res.isEmpty) dbQuery else res
  }

  def lookupBundle(hash: String): Option[Sheaf] = {
    // leveldb fix here
   /* def dbQuery = {
      dbActor.flatMap{ d => (d ? DBGet(hash)).mapTo[Option[Sheaf]].getOpt(t=5).flatten }
    }*/
    val res = bundleToSheaf.get(hash)
    //if (res.isEmpty) dbQuery else res
    res
  }

  def lookupBundle(bundle: Bundle): Option[Sheaf] = {
    lookupBundle(bundle.hash)
  }

  def lookupSheaf(sheaf: Sheaf): Option[Sheaf] = {
    lookupBundle(sheaf.bundle)
  }

  def putBundleDB(sheaf: Sheaf): Unit = {
    val hash = sheaf.bundle.hash
    dbActor.foreach{_ ! DBPut(hash, sheaf)}
  }

  def storeBundle(sheaf: Sheaf): Unit = {
    val hash = sheaf.bundle.hash
    bundleToSheaf(hash) = sheaf
    dbActor.foreach{_ ! DBPut(hash, sheaf)}
    // Try{db.put(bundleMetaData.bundle.hash, bundleMetaData)}
  }

  def processPeerSyncHeartbeat(psh: PeerSyncHeartbeat): Unit = {
    handleBundle(psh.maxBundle)
    // TODO: Find ID from ip for REST and UDP
//    psh.id.foreach{ r =>
    peerSync(psh.id) = psh
  }

  val depthScoreMap = Map(
    0 -> 1,
    1 -> 10,
    2 -> 100,
    3 -> 500,
    4 -> 2000
  )

  implicit class BundleExtData(b: Bundle) {

    def meta: Option[Sheaf] = lookupBundle(b.hash) //db.getAs[BundleMetaData](b.hash)

    def depthScore = {
      val depth = b.maxStackDepth
      depthScoreMap.get(depth) match {
        case Some(x) => x
        case None if depth < 0 => 1
        case None if depth > 4 => 5000
      }
    }

    def scoreFrom(m: Sheaf): Double = {
      val norm = normalizeReputations(m.reputations)
      val repScore = b.extractIds.toSeq.map { id => norm.getOrElse(id.b58, 0.1)}.sum
      depthScore + b.extractTXHash.size + repScore * 10
    }

    def score: Option[Double] = meta.flatMap{ m =>
      if (m.reputations.isEmpty) None
      else Some(scoreFrom(m))
    }

    def totalScore : Option[Double] = meta.flatMap{_.totalScore}

    def minTime: Long = meta.get.rxTime
    def pretty: String = s"hash: ${b.short}, depth: ${b.maxStackDepth}, numTX: ${b.extractTXHash.size}, " +
      s"numId: ${b.extractIds.size}, " +
      s"score: $score, totalScore: $totalScore, height: ${meta.map{_.height}}, " +
      s"parent: ${meta.map{_.bundle.extractParentBundleHash.pbHash.slice(0, 5)}} firstId: ${b.extractIds.head.short}"


    def extractTX: Set[Transaction] = b.extractTXHash.flatMap{ z => lookupTransaction(z.txHash)}
    def extractTXDB: Set[Transaction] = b.extractTXHash.flatMap{ z => lookupTransactionDBFallbackBlocking(z.txHash)}

    def reputationUpdate: Map[String, Long] = {
      b.extractIds.map{_.b58 -> 1L}.toMap
    }

  }

  implicit class SheafExt(s: Sheaf) {
    def parent: Option[Sheaf] = lookupBundle(s.bundle.extractParentBundleHash.pbHash)
  }

  val sheafResolveAdditionMonoid: Monoid[Sheaf] = new Monoid[Sheaf] {
    def empty: Sheaf = Sheaf(null)
    def combine(left: Sheaf, right: Sheaf): Sheaf = {
      if (!left.isResolved){
        right
      }
      else if (right.isResolved) right
      else {
        sheafUpdateAdditionMonoid.combine(left, right)
      }
    }
  }

  val sheafUpdateAdditionMonoid: Monoid[Sheaf] = new Monoid[Sheaf] {
    def empty: Sheaf = Sheaf(null)
    def combine(left: Sheaf, right: Sheaf): Sheaf = {

      val ids = right.bundle.extractIds.map{_.b58}
      val newReps = left.reputations.map { case (id, rep) =>
        id -> {
          if (ids.contains(id)) rep + 1 else rep
        }
      } ++ ids.filterNot(left.reputations.contains).map{_ -> 1L}.toMap

      val updatedRight = right.copy(
        height = Some(left.height.get + 1),
        reputations = newReps
      )

      val totalScoreLeft = left.totalScore.get
      val rightScore = updatedRight.bundle.scoreFrom(updatedRight)
      val updatedRightScore = updatedRight.copy(
        totalScore = Some(totalScoreLeft + rightScore)
      )
      storeBundle(updatedRightScore)
      updateMaxBundle(updatedRightScore)
      updatedRightScore
    }

  }


  def jaccard[T](t1: Set[T], t2: Set[T]): Double = {
    t1.intersect(t2).size.toDouble / t1.union(t2).size.toDouble
  }

  def prettifyBundle(b: Bundle): String = b.pretty

  // UNSAFE
  def findAncestors(
                     parentHash: String,
                     ancestors: Seq[Sheaf] = Seq()
                   ): Seq[Sheaf] = {
    val parent = lookupBundle(parentHash)
    if (parent.isEmpty) {
      if (parentHash != "coinbase") {
        syncPendingBundleHashes += parentHash
      }
      ancestors
    } else if (parentHash == genesisBundle.get.hash) {
      Seq(lookupBundle(genesisBundle.get.hash).get) ++ ancestors
    } else {
      findAncestors(
        parent.get.bundle.extractParentBundleHash.pbHash,
        Seq(parent.get) ++ ancestors
      )
    }
  }

  @memoize(1000, 600.seconds)
  private def findAncestorsUpToLastResolved(
                                             parentHash: String,
                                             ancestors: Seq[Sheaf] = Seq(),
                                             upTo: Int = 150
                                           ): Seq[Sheaf] = {
    val parent = lookupBundleDBFallbackBlocking(parentHash)
    def updatedAncestors: Seq[Sheaf] = parent.get +: ancestors
    if (parent.isEmpty) {
      if (parentHash != "coinbase") {
        syncPendingBundleHashes += parentHash
      }
      ancestors
    }
    else if (parent.get.isResolved) {
      updatedAncestors
    }
    else if (ancestors.size >= upTo) updatedAncestors
    else {
      findAncestorsUpToLastResolved(
        parent.get.bundle.extractParentBundleHash.pbHash,
        updatedAncestors,
        upTo
      )
    }
  }

  @memoize(1000, 600.seconds)
  private def findAncestorsUpToLastResolvedIterative(parentHash: String,
                                            maxDepth: Int = 2000): Seq[Sheaf] = {
    var currentSheaf = lookupBundleDBFallbackBlocking(parentHash)
    var ancestors: List[Sheaf] = List()
    var depth = 0
    while(depth < maxDepth && currentSheaf.exists(p => !p.isResolved)) {
      val parent = currentSheaf.get
      ancestors = parent +: ancestors
      val parentHash = parent.bundle.extractParentBundleHash.pbHash
      currentSheaf = lookupBundleDBFallbackBlocking(parentHash)
      if (currentSheaf.isEmpty) {
        if (parentHash != "coinbase") {
          syncPendingBundleHashes += parentHash
        }
      }
      depth += 1
    }

    currentSheaf.toList ++ ancestors
  }

  @memoize(1000, 600.seconds)
  def findAncestorsUpTo(
                         parentHash: String,
                         ancestors: Seq[Sheaf] = Seq(),
                         upTo: Int = 1
                       ): Seq[Sheaf] = {
    val parent = lookupBundleDBFallbackBlocking(parentHash)
    def updatedAncestors: Seq[Sheaf] = Seq(parent.get) ++ ancestors
    if (parent.isEmpty || updatedAncestors.size >= upTo) {
      ancestors
    }
    else {
      findAncestorsUpTo(
        parent.get.bundle.extractParentBundleHash.pbHash,
        updatedAncestors,
        upTo
      )
    }
  }



  def updateMaxBundle(sheaf: Sheaf): Unit = {

    val genCheck = totalNumValidatedTX == 1 || sheaf.bundle.maxStackDepth >= 2 // TODO : Possibly move this only to mempool emit

    if (sheaf.totalScore.get > maxBundle.get.totalScore.get && genCheck) {
      maxBundleMetaData.synchronized {
        maxBundleMetaData = Some(sheaf)
        val ancestors = findAncestorsUpTo(
          sheaf.bundle.extractParentBundleHash.pbHash,
          upTo = 100
        )
        val height = sheaf.height.get
        if (height > confirmWindow) {
          if (downloadInProgress) {
            downloadInProgress = false
            downloadMode = false
          }
          totalNumValidBundles = height - confirmWindow
        }

        last100ValidBundleMetaData = if (ancestors.size < confirmWindow + 1) Seq()
        else ancestors.take(ancestors.size - confirmWindow)
        val newTX = last100ValidBundleMetaData.takeRight(confirmWindow).flatMap(_.bundle.extractTXHash).toSet
        txInMaxBundleNotInValidation = newTX.map{_.txHash}
          // .filter { h => !last10000ValidTXHash.contains(h) }

     //   if (height % 10 == 0) {
          newTX.foreach(t => lookupTransactionDBFallbackBlocking(t.txHash).foreach {acceptTransaction})
       // }

      }
    }

    // Set this to be active for the combiners.
    if (!activeDAGBundles.contains(sheaf) &&
      !sheaf.bundle.extractIds.contains(id) && sheaf.bundle != genesisBundle.get && !downloadMode && !downloadInProgress) {
      activeDAGManager.acceptSheaf(sheaf)
      //activeDAGBundles :+= bundleMetaData
    }
  }

  def updateBundleFrom(left: Sheaf, right: Sheaf): Sheaf = {
    sheafUpdateAdditionMonoid.combine(left, right)
  }

  def attemptResolveBundle(zero: Sheaf, parentHash: String): Unit = {

    val ancestors = findAncestorsUpToLastResolvedIterative(parentHash)
    if (lookupSheaf(zero).isEmpty) storeBundle(zero)

    if (ancestors.nonEmpty) {

      val chainR = ancestors.tail ++ Seq(zero)

      val chain = chainR.map{
        c =>
          val res = resolveTransactions(c.bundle.extractTXHash.map{_.txHash})
          // println(s"RESOLVE TRANSACTIONS: $res ${c.bundle.hash.slice(0, 5)}")
          c.copy(transactionsResolved = res)
        //if (c.transactionsResolved != res) {
        //            db.put(c.bundle.hash, c.copy(transactionsResolved = res))
        //        }
      }

      //  println("MONOID SHEAF INVOKED")
      //  ancestors.reduce(sheafAdditionMonoid.combine)

      // TODO: Change to recursion to implement abort-early fold

      chain.fold(ancestors.head) { sheafResolveAdditionMonoid.combine}


    }

    /* println(s"RX on ${id.short} BUNDLE ${zero.bundle.hash.slice(0, 5)} " +
       s"${zero.isResolved} ${zero.height} ${zero.totalScore}" +
       s" ${parentHash.slice(0, 5)} ${db.contains(zero.bundle.hash)} ${ancestors.map{
       a => a.bundle.hash.slice(0, 5) + "_txResolved:" + a.transactionsResolved + "_" + a.totalScore}
       }" )
 */

  }

  def resolveTransactions(txs: Set[String]): Boolean = {
    val missing = txs.filter(z => lookupTransaction(z).isEmpty)
    syncPendingTXHashes ++= missing
    missing.foreach(m => txSyncRequestTime(m) = System.currentTimeMillis())
    missing.isEmpty
  }


  def handleBundle(bundle: Bundle): Unit = {


    if (syncPendingBundleHashes.contains(bundle.hash)) {
      numSyncedBundles += 1
      syncPendingBundleHashes -= bundle.hash
    }

    totalNumBundleMessages += 1
    val parentHash = bundle.extractParentBundleHash.pbHash
    val bmd = lookupBundle(bundle.hash)
    val notPresent = bmd.isEmpty

    val txs = bundle.extractTXHash.map{_.txHash}
    if (txs.nonEmpty) {

      val txResolved = resolveTransactions(txs)


      val zero = if (notPresent) {
        totalNumNewBundleAdditions += 1
        Sheaf(bundle, transactionsResolved = txResolved)
      } else {
        bmd.get.copy(transactionsResolved = txResolved)
      }

      if (zero.isResolved) {
        updateMaxBundle(zero)
      } else {
        attemptResolveBundle(zero, parentHash)
      }
    }

  }
}
