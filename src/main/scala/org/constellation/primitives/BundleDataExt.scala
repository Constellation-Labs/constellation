package org.constellation.primitives

import org.constellation.LevelDB
import org.constellation.consensus.Consensus.{CC, RoundHash}
import org.constellation.primitives.Schema._
import org.constellation.util.Signed

import scala.collection.concurrent.TrieMap

trait BundleDataExt extends Reputation {

  @volatile var db: LevelDB

  var genesisBundle : Bundle = _
  def genesisTXHash: String = genesisBundle.extractTX.head.hash

  @volatile var last100ValidBundleMetaData : Seq[BundleMetaData] = Seq()
  def lastValidBundle: Bundle = last100ValidBundleMetaData.last.bundle
  def lastValidBundleHash = ParentBundleHash(lastValidBundle.hash)

  @volatile var maxBundleMetaData: BundleMetaData = _
  def maxBundle: Bundle = maxBundleMetaData.bundle

  @volatile var syncPendingBundleHashes: Set[String] = Set()
  @volatile var syncPendingTXHashes: Set[String] = Set()

  @volatile var activeDAGBundles: Seq[Bundle] = Seq[Bundle]()

  @volatile var linearCheckpointBundles: Set[Bundle] = Set[Bundle]()
  val checkpointsInProgress: TrieMap[RoundHash[_ <: CC], Boolean] = TrieMap()

  implicit class BundleExtData(b: Bundle) {
    def meta: Option[BundleMetaData] = db.getAs[BundleMetaData](b.hash)
    def score: Option[Double] = meta.map{ m =>
      val norm = normalizeReputations(m.reputations)
      val repScore = b.extractIds.toSeq.map { id => norm.getOrElse(id, 0.1)}.sum
      b.maxStackDepth * 300 + extractTXHash.size + repScore * 10
    }
    def totalScore : Option[Double] = score.map{
      _ + meta.get.totalScore.get
    }
    def minTime: Long = meta.get.rxTime
    def pretty: String = s"hash: ${b.short}, depth: ${b.maxStackDepth}, numTX: ${extractTXHash.size}, " +
      s"numId: ${b.extractIds.size}, " +
      s"score: $score, totalScore: $totalScore, height: ${meta.map{_.height}}, " +
      s"parent: ${meta.map{_.bundle.extractParentBundleHash.pbHash.slice(0, 5)}}"

    def extractTXHash: Set[TransactionHash] = {
      def process(s: Signed[BundleData]): Set[TransactionHash] = {
        val bd = s.data.bundles
        val depths = bd.map {
          case b2: Bundle =>
            process(b2.bundleData)
          case h: TransactionHash => Set(h)
          case _ => Set[TransactionHash]()
        }
        if (depths.nonEmpty) {
          depths.reduce( (s1: Set[TransactionHash], s2: Set[TransactionHash]) => s1 ++ s2)
        } else {
          Set[TransactionHash]()
        }
      }
      process(b.bundleData)
    }

    def extractTX: Set[TX] = extractTXHash.flatMap{ z => db.getAs[TX](z.txHash)}

    def reputationUpdate: Map[Id, Long] = {
      b.extractIds.map{_ -> 1L}.toMap
    }

  }

  def jaccard[T](t1: Set[T], t2: Set[T]): Double = {
    t1.intersect(t2).size.toDouble / t1.union(t2).size.toDouble
  }

  def prettifyBundle(b: Bundle): String = b.pretty

  def findAncestors(
                     parentHash: String,
                     ancestors: Seq[BundleMetaData] = Seq()
                   ): Seq[BundleMetaData] = {
    val parent = db.getAs[BundleMetaData](parentHash)
    if (parent.isEmpty) {
      syncPendingBundleHashes += parentHash
      ancestors
    } else if (parentHash == genesisBundle.hash) {
      Seq(db.getAs[BundleMetaData](genesisBundle.hash).get) ++ ancestors
    } else {
      findAncestors(
        parent.get.bundle.extractParentBundleHash.pbHash,
        Seq(parent.get) ++ ancestors
      )
    }
  }

  def findAncestorsUpToLastResolved(
                                     parentHash: String,
                                     ancestors: Seq[BundleMetaData] = Seq()
                                   ): Seq[BundleMetaData] = {
    val parent = db.getAs[BundleMetaData](parentHash)
    val updatedAncestors = Seq(parent.get) ++ ancestors
    if (parent.isEmpty) {
      syncPendingBundleHashes += parentHash
      ancestors
    } else if (parent.get.isResolved) updatedAncestors
    else {
      findAncestors(
        parent.get.bundle.extractParentBundleHash.pbHash,
        updatedAncestors
      )
    }
  }

  def findAncestorsUpTo(
                         parentHash: String,
                         ancestors: Seq[BundleMetaData] = Seq(),
                         upTo: Int = 1
                       ): Seq[BundleMetaData] = {
    val parent = db.getAs[BundleMetaData](parentHash)
    val updatedAncestors = Seq(parent.get) ++ ancestors
    if (parent.isEmpty || updatedAncestors.size >= upTo) {
      ancestors
    }
    else {
      findAncestors(
        parent.get.bundle.extractParentBundleHash.pbHash,
        updatedAncestors
      )
    }
  }

  def acceptTransaction(tx: Schema.TX)

  def updateMaxBundle(bundleMetaData: BundleMetaData): Unit = {
    if (bundleMetaData.totalScore.get > maxBundle.totalScore.get) {
      maxBundleMetaData = bundleMetaData
      val ancestors = findAncestorsUpTo(
        bundleMetaData.bundle.extractParentBundleHash.pbHash,
        upTo = 100
      )
      last100ValidBundleMetaData = if (ancestors.size < 6 ) Seq()
      else ancestors.slice(0, ancestors.size - 5)
      val newTX = last100ValidBundleMetaData.reverse.slice(0, 5).flatMap(_.bundle.extractTX)
      newTX.foreach(t => acceptTransaction(t))
    }
  }

  def attemptResolveBundle(zero: BundleMetaData, parentHash: String): Unit = {

    val ancestors = findAncestorsUpToLastResolved(parentHash)

    if (ancestors.nonEmpty) {

      val chain = ancestors.tail ++ Seq(zero)

      // TODO: Change to recursion to implement abort-early fold
      chain.fold(ancestors.head) {
        case (left, right) =>

          if (!left.isResolved) right
          else if (right.isResolved) right
          else {

            val ids = right.bundle.extractIds
            val newReps = left.reputations.map { case (id, rep) =>
              id -> {
                if (ids.contains(id)) rep + 1 else rep
              }
            }

            val updatedRight = right.copy(
              height = Some(left.height.get + 1),
              reputations = newReps
            )

            val updatedRightScore = updatedRight.copy(
              totalScore = Some(left.totalScore.get + updatedRight.bundle.score.get)
            )
            db.put(right.bundle.hash, updatedRightScore)
            updateMaxBundle(updatedRightScore)
            updatedRightScore
          }
      }

    } else {
      db.put(zero.bundle.hash, zero)
    }

  }

}
