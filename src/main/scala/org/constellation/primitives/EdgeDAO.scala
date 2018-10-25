package org.constellation.primitives

import java.util.concurrent.{Executors, TimeUnit}

import akka.util.Timeout
import org.constellation.consensus.EdgeProcessor.acceptCheckpoint
import org.constellation.consensus.{Snapshot, SnapshotInfo, TipData}
import org.constellation.primitives.Schema._
import org.constellation.{DAO, ProcessingConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Random, Success, Try}


class ThreadSafeTXMemPool() {

  private var transactions = Seq[Transaction]()

  def pull(minCount: Int): Option[Seq[Transaction]] = this.synchronized{
    if (transactions.size > minCount) {
      val (left, right) = transactions.splitAt(minCount)
      transactions = right
      Some(left)
    } else None
  }

  def batchPutDebug(txs: Seq[Transaction]) : Boolean = this.synchronized{
    transactions ++= txs
    true
  }

  def put(transaction: Transaction)(implicit dao: DAO): Boolean = this.synchronized{
    val notContained = !transactions.contains(transaction)
    if (notContained && transactions.size < dao.processingConfig.maxMemPoolSize) {
      transactions :+= transaction
    }
    notContained
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

  var totalNumCBsInShapshots = 0L

  def attemptSnapshot()(implicit dao: DAO): Unit = this.synchronized{

    val peerIds = dao.peerInfo //(dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq
    val facilMap = peerIds.filter{case (_, pd) =>
      pd.timeAdded < (System.currentTimeMillis() - dao.processingConfig.minPeerTimeAddedSeconds * 1000) && pd.nodeState == NodeState.Ready
    }

    facilitators = facilMap

    if (dao.nodeState == NodeState.Ready && acceptedCBSinceSnapshot.nonEmpty) {

      val nextSnapshot = Snapshot(snapshot.hash, acceptedCBSinceSnapshot)

      // TODO: Make this a future and have it not break the unit test
      // Also make the db puts blocking, may help for different issue
      if (snapshot != Snapshot.snapshotZero) {
        dao.metricsManager ! IncrementMetric("snapshotCount")
        Snapshot.acceptSnapshot(snapshot)
        totalNumCBsInShapshots += snapshot.checkpointBlocks.size
        dao.metricsManager ! UpdateMetric("totalNumCBsInShapshots", totalNumCBsInShapshots.toString)
      }

      if (snapshot.lastSnapshot != Snapshot.snapshotZeroHash && snapshot.lastSnapshot != "") {
        val lastSnapshotVerification = dao.dbActor.getSnapshot(snapshot.lastSnapshot)
        if (lastSnapshotVerification.isEmpty) {
          dao.metricsManager ! IncrementMetric("snapshotVerificationFailed")
        } else {
          dao.metricsManager ! IncrementMetric("snapshotVerificationCount")
          if (
            !lastSnapshotVerification.get.checkpointBlocks.map{dao.dbActor.getCheckpointCacheData}.forall(_.nonEmpty)
          ) {
            dao.metricsManager ! IncrementMetric("snapshotCBVerificationFailed")
          } else {
            dao.metricsManager ! IncrementMetric("snapshotCBVerificationCount")
          }

        }
      }

      snapshot = nextSnapshot
      acceptedCBSinceSnapshot = Seq()
      dao.metricsManager ! UpdateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
    }
  }

  def acceptGenesis(genesisObservation: GenesisObservation): Unit = this.synchronized{
    thresholdMetCheckpoints += genesisObservation.initialDistribution.baseHash -> TipData(genesisObservation.initialDistribution, 0)
    thresholdMetCheckpoints += genesisObservation.initialDistribution2.baseHash -> TipData(genesisObservation.initialDistribution2, 0)
  }

  def pull()(implicit dao: DAO): Option[(Seq[SignedObservationEdge], Map[Id, PeerData])] = this.synchronized{
    if (thresholdMetCheckpoints.size >= 2 && facilitators.nonEmpty) {
      val tips = Random.shuffle(thresholdMetCheckpoints.toSeq).take(2)

      val tipSOE = tips.map {
        _._2.checkpointBlock.checkpoint.edge.signedObservationEdge
      }.sortBy(_.hash)

      val mergedTipHash = tipSOE.map {_.hash}.mkString("")

      val totalNumFacil = facilitators.size
      // TODO: Use XOR distance instead as it handles peer data mismatch cases better
      val facilitatorIndex = (BigInt(mergedTipHash, 16) % totalNumFacil).toInt
      val sortedFacils = facilitators.toSeq.sortBy(_._1.encodedId.b58Encoded)
      val selectedFacils = Seq.tabulate(dao.processingConfig.numFacilitatorPeers) { i => (i + facilitatorIndex) % totalNumFacil }.map {
        sortedFacils(_)
      }
      val finalFacilitators = selectedFacils.toMap
      dao.metricsManager ! UpdateMetric("activeTips", thresholdMetCheckpoints.size.toString)

      Some(tipSOE -> finalFacilitators)
    } else None
  }


  def accept(checkpointBlock: CheckpointBlock)(implicit dao: DAO): Unit = this.synchronized {

    // Below should be future, turned off for sanity checking
    Try{ acceptCheckpoint(checkpointBlock) } match {
      case Success(x) =>
        dao.metricsManager ! IncrementMetric("acceptCheckpointSuccess")
      case Failure(e) =>
        e.printStackTrace()
        dao.metricsManager ! IncrementMetric("acceptCheckpointFailure")
    }

 //(dao.edgeExecutionContext)

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
    dao.metricsManager ! UpdateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
  }

}


trait EdgeDAO {


  val threadSafeTXMemPool = new ThreadSafeTXMemPool()
  val threadSafeTipService = new ThreadSafeTipService()

  var snapshotInterval: Int = 30

  var genesisObservation: Option[GenesisObservation] = None
  def maxWidth: Int = processingConfig.maxWidth
  def minCheckpointFormationThreshold: Int = processingConfig.minCheckpointFormationThreshold
  def minCBSignatureThreshold: Int = processingConfig.numFacilitatorPeers

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

  val signatureResponsePool: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  val txProcessorPool: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  def canCreateCheckpoint: Boolean = {
    transactionMemPool.size >= minCheckpointFormationThreshold && checkpointMemPoolThresholdMet.size >= 2
  }

  def reuseTips: Boolean = checkpointMemPoolThresholdMet.size < maxWidth


  // Temporary to get peer data for tx hash partitioning
  @volatile var peerInfo: Map[Id, PeerData] = Map()


}
