package org.constellation.primitives

import java.util.concurrent.{Executors, TimeUnit}

import akka.util.Timeout
import better.files.File
import com.twitter.storehaus.cache.LRUCache
import org.constellation.consensus.EdgeProcessor.acceptCheckpoint
import org.constellation.consensus._
import org.constellation.primitives.Schema._
import org.constellation.serializer.KryoSerializer
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

  private val checkpointCache = {
    import com.twitter.storehaus.cache._
    LRUCache[String, CheckpointBlock](5000)
  }

  def tips: Map[String, TipData] = thresholdMetCheckpoints

  def getSnapshotInfo: SnapshotInfo = this.synchronized(SnapshotInfo(snapshot, acceptedCBSinceSnapshot))

  var totalNumCBsInShapshots = 0L

  // ONLY TO BE USED BY DOWNLOAD COMPLETION CALLER
  def setSnapshot(latestSnapshot: Snapshot): Unit = this.synchronized{
    snapshot = latestSnapshot
    // Below may not be necessary, just a sanity check
    acceptedCBSinceSnapshot = acceptedCBSinceSnapshot.filterNot(snapshot.checkpointBlocks.contains)
  }

  def attemptSnapshot()(implicit dao: DAO): Unit = this.synchronized{

    // Sanity check memory protection
    if (thresholdMetCheckpoints.size > dao.processingConfig.maxActiveTipsAllowedInMemory) {
      thresholdMetCheckpoints = thresholdMetCheckpoints.slice(0, 100)
      dao.metricsManager ! IncrementMetric("memoryExceeded_thresholdMetCheckpoints")
      dao.metricsManager ! UpdateMetric("activeTips", thresholdMetCheckpoints.size.toString)
    }
    if (acceptedCBSinceSnapshot.size > dao.processingConfig.maxAcceptedCBHashesInMemory) {
      acceptedCBSinceSnapshot = acceptedCBSinceSnapshot.slice(0, 100)
      dao.metricsManager ! IncrementMetric("memoryExceeded_acceptedCBSinceSnapshot")
      dao.metricsManager ! UpdateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
    }


    val peerIds = dao.peerInfo //(dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq
    val facilMap = peerIds.filter{case (_, pd) =>
      pd.peerMetadata.timeAdded < (System.currentTimeMillis() - dao.processingConfig.minPeerTimeAddedSeconds * 1000) && pd.peerMetadata.nodeState == NodeState.Ready
    }

    facilitators = facilMap

    if (dao.nodeState == NodeState.Ready && acceptedCBSinceSnapshot.nonEmpty) {

      val nextSnapshot = Snapshot(snapshot.hash, acceptedCBSinceSnapshot.sorted)

      // TODO: Make this a future and have it not break the unit test
      // Also make the db puts blocking, may help for different issue
      if (snapshot != Snapshot.snapshotZero) {
        dao.metricsManager ! IncrementMetric("snapshotCount")

        // Write snapshot to file
        tryWithMetric({
          val maybeBlocks = snapshot.checkpointBlocks.map {h =>
            val res = checkpointCache.get(h)
            if (res.isEmpty) dao.dbActor.getCheckpointCacheData(h).map{_.checkpointBlock}
            else res
          }
          if (maybeBlocks.exists(_.isEmpty)) {
            dao.metricsManager ! IncrementMetric("snapshotWriteToDiskMissingData")
          }
          File(dao.snapshotPath, snapshot.hash).writeByteArray(KryoSerializer.serializeAnyRef(StoredSnapshot(snapshot, maybeBlocks.flatten)))
        },
          "snapshotWriteToDisk"
        )

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


  // TODO: Synchronize only on values modified by this, same for other functions
  def accept(checkpointBlock: CheckpointBlock)(implicit dao: DAO): Unit = this.synchronized {

    checkpointCache.put(checkpointBlock.baseHash, checkpointBlock)

    tryWithMetric(acceptCheckpoint(checkpointBlock), "acceptCheckpoint")

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
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10))

  val signatureResponsePool: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10))

  val txProcessorPool: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10))

  def canCreateCheckpoint: Boolean = {
    transactionMemPool.size >= minCheckpointFormationThreshold && checkpointMemPoolThresholdMet.size >= 2
  }

  def reuseTips: Boolean = checkpointMemPoolThresholdMet.size < maxWidth


  // Temporary to get peer data for tx hash partitioning
  @volatile var peerInfo: Map[Id, PeerData] = Map()


}
