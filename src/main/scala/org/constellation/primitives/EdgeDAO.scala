package org.constellation.primitives

import java.util.concurrent.{Executors, TimeUnit}

import akka.util.Timeout
import org.constellation.consensus.EdgeProcessor.acceptCheckpoint
import org.constellation.consensus.{Snapshot, SnapshotInfo, TipData}
import org.constellation.primitives.Schema._
import org.constellation.{DAO, ProcessingConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Random


class ThreadSafeTXMemPool() {

  private var transactions = Seq[Transaction]()

  def pull(minCount: Int): Option[Seq[Transaction]] = this.synchronized{
    if (transactions.size > minCount) {
      val (left, right) = transactions.splitAt(minCount)
      transactions = right
      Some(left)
    } else None
  }

  def put(transaction: Transaction): Boolean = this.synchronized{
    if (!transactions.contains(transaction)) {
      if (transactions.size < 1000) {
        transactions :+= transaction
      }
      true
    } else false
  }

  def unsafeCount: Int = transactions.size

}

import akka.pattern.ask
import constellation._

class ThreadSafeTipService() {

  implicit val timeout: Timeout = Timeout(15, TimeUnit.SECONDS)


  private var thresholdMetCheckpoints: Map[String, TipData] = Map()
  private var acceptedCBSinceSnapshot: Seq[String] = Seq()
  private var facilitators: Map[Id, PeerData] = Map()
  private var snapshot: Snapshot = Snapshot.snapshotZero

  def getSnapshotInfo: SnapshotInfo = this.synchronized(SnapshotInfo(snapshot, acceptedCBSinceSnapshot))

  def attemptSnapshot()(implicit dao: DAO): Unit = this.synchronized{

    val peerIds = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq
    val facilMap = peerIds.filter{case (_, pd) =>
      pd.timeAdded < (System.currentTimeMillis() - 30*1000) && pd.nodeState == NodeState.Ready
    }.toMap

    facilitators = facilMap

    if (dao.nodeState == NodeState.Ready && acceptedCBSinceSnapshot.nonEmpty) {

      val nextSnapshot = Snapshot(snapshot.hash, acceptedCBSinceSnapshot)
      Snapshot.acceptSnapshot(snapshot)

      if (snapshot.lastSnapshot != Snapshot.snapshotZeroHash) {
        val lastSnapshotVerification = dao.dbActor.getSnapshot(snapshot.lastSnapshot)
        if (lastSnapshotVerification.isEmpty) {
          dao.metricsManager ! IncrementMetric("snapshotVerificationFailed")
        } else {
          dao.metricsManager ! IncrementMetric("snapshotVerificationCount")
        }
      }
      dao.metricsManager ! IncrementMetric("snapshotCount")
      snapshot = nextSnapshot
      acceptedCBSinceSnapshot = Seq()
    }
  }

  def acceptGenesis(genesisObservation: GenesisObservation): Unit = this.synchronized{
    thresholdMetCheckpoints += genesisObservation.initialDistribution.baseHash -> TipData(genesisObservation.initialDistribution, 0)
    thresholdMetCheckpoints += genesisObservation.initialDistribution2.baseHash -> TipData(genesisObservation.initialDistribution2, 0)
  }

  def pull(): Option[(Seq[SignedObservationEdge], Set[Id])] = this.synchronized{
    if (thresholdMetCheckpoints.size >= 2 && facilitators.nonEmpty) {
      val tips = Random.shuffle(thresholdMetCheckpoints.toSeq).take(2)

      val tipSOE = tips.map {
        _._2.checkpointBlock.checkpoint.edge.signedObservationEdge
      }.sortBy(_.hash)

      tipSOE.foreach{ soe =>
        thresholdMetCheckpoints -= soe.baseHash
      }

      val mergedTipHash = tipSOE.map {_.hash}.mkString("")

      val totalNumFacil = facilitators.size
      // TODO: Use XOR distance instead as it handles peer data mismatch cases better
      val facilitatorIndex = (BigInt(mergedTipHash, 16) % totalNumFacil).toInt
      val sortedFacils = facilitators.toSeq.sortBy(_._1.encodedId.b58Encoded)
      val selectedFacils = Seq.tabulate(2) { i => (i + facilitatorIndex) % totalNumFacil }.map {
        sortedFacils(_)
      }
      val finalFacilitators = selectedFacils.map {_._1}.toSet
      Some(tipSOE -> finalFacilitators)
    } else None
  }

  def accept(checkpointBlock: CheckpointBlock)(implicit dao: DAO): Unit = this.synchronized {

    acceptCheckpoint(checkpointBlock)

    def reuseTips: Boolean = thresholdMetCheckpoints.size < dao.maxWidth

    val keysToRemove = checkpointBlock.parentSOEBaseHashes.flatMap {
      h =>
        thresholdMetCheckpoints.get(h).flatMap {
          case TipData(block, numUses) =>

            def doRemove(): Option[String] = {
              dao.metricsManager ! IncrementMetric("checkpointTipsRemoved")
              Some(block.baseHash)
            }

            if (reuseTips) {
              if (numUses >= 2) {
                doRemove()
              } else {
                None
              }
            } else {
              doRemove()
            }
        }
    }

    val keysToUpdate = checkpointBlock.parentSOEBaseHashes.flatMap {
      h =>
        thresholdMetCheckpoints.get(h).flatMap {
          case TipData(block, numUses) =>

            def doUpdate(): Option[(String, TipData)] = {
              dao.metricsManager ! IncrementMetric("checkpointTipsIncremented")
              Some(block.baseHash -> TipData(block, numUses + 1))
            }

            if (reuseTips && numUses <= 2) {
              doUpdate()
            } else None
        }
    }.toMap

    thresholdMetCheckpoints = thresholdMetCheckpoints +
      (checkpointBlock.baseHash -> TipData(checkpointBlock, 0)) ++
      keysToUpdate --
      keysToRemove

    acceptedCBSinceSnapshot = acceptedCBSinceSnapshot :+ checkpointBlock.baseHash
  }

}


trait EdgeDAO {


  val threadSafeTXMemPool = new ThreadSafeTXMemPool()
  val threadSafeTipService = new ThreadSafeTipService()

  var snapshotInterval: Int = 30

  var genesisObservation: Option[GenesisObservation] = None
  def maxWidth: Int = processingConfig.maxWidth
  def minCheckpointFormationThreshold: Int = processingConfig.minCheckpointFormationThreshold
  def minCBSignatureThreshold: Int = processingConfig.minCBSignatureThreshold

  val minTXSignatureThreshold = 5
  val maxUniqueTXSize = 500
  val maxNumSignaturesPerTX = 20

  var processingConfig = ProcessingConfig()

  @volatile var transactionMemPool: Seq[Transaction] = Seq()

  val transactionMemPoolMultiWitness : TrieMap[String, Transaction] = TrieMap()
  val checkpointMemPool : TrieMap[String, CheckpointBlock] = TrieMap()

  // TODO: temp
  val confirmedCheckpoints: TrieMap[String, CheckpointBlock] = TrieMap()

  @volatile var transactionMemPoolThresholdMet: Set[String] = Set()

  // Map from checkpoint hash to number of times used as a tip (number of children)
  val checkpointMemPoolThresholdMet: TrieMap[String, (CheckpointBlock, Int)] = TrieMap()



  val resolveNotifierCallbacks: TrieMap[String, Seq[CheckpointBlock]] = TrieMap()

  val edgeExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(20))

  def canCreateCheckpoint: Boolean = {
    transactionMemPool.size >= minCheckpointFormationThreshold && checkpointMemPoolThresholdMet.size >= 2
  }

  def reuseTips: Boolean = checkpointMemPoolThresholdMet.size < maxWidth


  // Temporary to get peer data for tx hash partitioning
  @volatile var peerInfo: Map[Id, PeerData] = Map()


}
