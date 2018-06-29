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

  @volatile var latest100ValidBundles : Seq[Bundle] = Seq()
  def lastValidBundle: Bundle = latest100ValidBundles.last
  def lastValidBundleHash = ParentBundleHash(lastValidBundle.hash)

  @volatile var bestBundle: Bundle = _

  @volatile var syncPendingBundleHashes: Set[String] = Set()
  @volatile var syncPendingTXHashes: Set[String] = Set()

  @volatile var activeDAGBundles: Seq[Bundle] = Seq[Bundle]()

  @volatile var memPool: Set[String] = Set()

  @volatile var last100SelfSentTransactions: Seq[TX] = Seq()
  @volatile var last1000ValidTX: Seq[String] = Seq()

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

}
