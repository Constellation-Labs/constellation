package org.constellation.primitives

import java.util.concurrent.{Executors, TimeUnit}

import akka.util.Timeout
import better.files.File
import com.twitter.storehaus.cache.MutableLRUCache
import org.constellation.consensus.SnapshotTrigger.acceptCheckpoint
import org.constellation.consensus._
import org.constellation.primitives.Schema._
import org.constellation.serializer.KryoSerializer
import org.constellation.{DAO, ProcessingConfig}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
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

  def batchPutDebug(txs: Seq[Transaction]) : Boolean = this.synchronized{
    transactions ++= txs
    true
  }

  def put(transaction: Transaction, overrideLimit: Boolean = false)(implicit dao: DAO): Boolean = this.synchronized{
    val notContained = !transactions.contains(transaction)

    if (notContained) {
      if (overrideLimit) {
        // Prepend in front to process user TX first before random ones
        transactions = Seq(transaction) ++ transactions

      } else if (transactions.size < dao.processingConfig.maxMemPoolSize) {
        transactions :+= transaction
      }
    }
    notContained
  }

  def unsafeCount: Int = transactions.size

}

import constellation._

class ThreadSafeTipService() {

  implicit val timeout: Timeout = Timeout(15, TimeUnit.SECONDS)


  private var thresholdMetCheckpoints: Map[String, TipData] = Map()
  private var acceptedCBSinceSnapshot: Seq[String] = Seq()
  private var facilitators: Map[Id, PeerData] = Map()
  private var snapshot: Snapshot = Snapshot.snapshotZero

  def tips: Map[String, TipData] = thresholdMetCheckpoints

  def getSnapshotInfo()(implicit dao: DAO): SnapshotInfo = this.synchronized(
    SnapshotInfo(
      snapshot,
      acceptedCBSinceSnapshot,
      lastSnapshotHeight = lastSnapshotHeight,
      snapshotHashes = dao.snapshotHashes,
      addressCacheData = dao.addressService.lruCache.iterator.toMap,
      tips = thresholdMetCheckpoints,
      snapshotCache = snapshot.checkpointBlocks.flatMap{dao.checkpointService.get}
    )
  )

  var totalNumCBsInShapshots = 0L

  // ONLY TO BE USED BY DOWNLOAD COMPLETION CALLER
  def setSnapshot(latestSnapshotInfo: SnapshotInfo)(implicit dao: DAO): Unit = this.synchronized{
    snapshot = latestSnapshotInfo.snapshot
    lastSnapshotHeight = latestSnapshotInfo.lastSnapshotHeight
    thresholdMetCheckpoints = latestSnapshotInfo.tips

    // Below may not be necessary, just a sanity check
    acceptedCBSinceSnapshot = latestSnapshotInfo.acceptedCBSinceSnapshot
    latestSnapshotInfo.addressCacheData.foreach{
      case (k,v) =>
        dao.addressService.put(k, v)
    }

    latestSnapshotInfo.snapshotCache.foreach{
      h =>
        dao.metricsManager ! IncrementMetric("checkpointAccepted")
        dao.checkpointService.put(h.checkpointBlock.get.baseHash, h)
        h.checkpointBlock.get.transactions.foreach{
          _ =>
            dao.metricsManager ! IncrementMetric("transactionAccepted")
        }
    }

    latestSnapshotInfo.acceptedCBSinceSnapshotCache.foreach{
      h =>
        dao.checkpointService.put(h.checkpointBlock.get.baseHash, h)
        dao.metricsManager ! IncrementMetric("checkpointAccepted")
        h.checkpointBlock.get.transactions.foreach{
          _ =>
          dao.metricsManager ! IncrementMetric("transactionAccepted")
        }
    }

    dao.metricsManager ! UpdateMetric(
      "acceptCBCacheMatchesAcceptedSize",
      (latestSnapshotInfo.acceptedCBSinceSnapshot.size ==
        latestSnapshotInfo.acceptedCBSinceSnapshotCache.size).toString
    )


  }

  // TODO: Read from lastSnapshot in DB optionally, assign elsewhere
  var lastSnapshotHeight = 0

  def getMinTipHeight()(implicit dao: DAO) = thresholdMetCheckpoints.keys.map {
    dao.checkpointService.get
  }.flatMap {
    _.flatMap {
      _.height.map {
        _.min
      }
    }
  }.min

  var syncBuffer : Seq[CheckpointCacheData] = Seq()

  def syncBufferAccept(cb: CheckpointCacheData)(implicit dao: DAO): Unit = this.synchronized{
    syncBuffer :+= cb
    dao.metricsManager ! UpdateMetric("syncBufferSize", syncBuffer.size.toString)
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

      val minTipHeight = getMinTipHeight()
      dao.metricsManager ! UpdateMetric("minTipHeight", minTipHeight.toString)

      val nextHeightInterval = lastSnapshotHeight + dao.processingConfig.snapshotHeightInterval

      val canSnapshot = minTipHeight > (nextHeightInterval + dao.processingConfig.snapshotHeightDelayInterval)
      if (!canSnapshot) {
        dao.metricsManager ! IncrementMetric("snapshotHeightIntervalConditionNotMet")
      } else {

        val maybeDatas = acceptedCBSinceSnapshot.map(dao.checkpointService.get)

        val blocksWithinHeightInterval = maybeDatas.filter {
          _.exists(_.height.exists { h =>
            h.min > lastSnapshotHeight && h.min <= nextHeightInterval
          })
        }

        if (blocksWithinHeightInterval.isEmpty) {
          dao.metricsManager ! IncrementMetric("snapshotNoBlocksWithinHeightInterval")
        } else {

          val blockCaches = blocksWithinHeightInterval.map {
            _.get
          }

          val hashesForNextSnapshot = blockCaches.map {
            _.checkpointBlock.get.baseHash
          }.sorted
          val nextSnapshot = Snapshot(snapshot.hash, hashesForNextSnapshot)

          // TODO: Make this a future and have it not break the unit test
          // Also make the db puts blocking, may help for different issue
          if (snapshot != Snapshot.snapshotZero) {
            dao.metricsManager ! IncrementMetric("snapshotCount")

            // Write snapshot to file
            tryWithMetric({
              val maybeBlocks = snapshot.checkpointBlocks.map {
                dao.checkpointService.get
              }
              if (maybeBlocks.exists(_.exists(_.checkpointBlock.isEmpty))) {
                // TODO : This should never happen, if it does we need to reset the node state and redownload
                dao.metricsManager ! IncrementMetric("snapshotWriteToDiskMissingData")
              }
              val flatten = maybeBlocks.flatten.sortBy(_.checkpointBlock.map {
                _.baseHash
              })
              File(dao.snapshotPath, snapshot.hash).writeByteArray(KryoSerializer.serializeAnyRef(StoredSnapshot(snapshot, flatten)))
              // dao.dbActor.kvdb.put("latestSnapshot", snapshot)
            },
              "snapshotWriteToDisk"
            )

            Snapshot.acceptSnapshot(snapshot)
            dao.checkpointService.delete(snapshot.checkpointBlocks.toSet)


            totalNumCBsInShapshots += snapshot.checkpointBlocks.size
            dao.metricsManager ! UpdateMetric("totalNumCBsInShapshots", totalNumCBsInShapshots.toString)
            dao.metricsManager ! UpdateMetric("lastSnapshotHash", snapshot.hash)
          }

          // TODO: Verify from file
          /*
        if (snapshot.lastSnapshot != Snapshot.snapshotZeroHash && snapshot.lastSnapshot != "") {

          val lastSnapshotVerification = File(dao.snapshotPath, snapshot.lastSnapshot).read
          if (lastSnapshotVerification.isEmpty) {
            dao.metricsManager ! IncrementMetric("snapshotVerificationFailed")
          } else {
            dao.metricsManager ! IncrementMetric("snapshotVerificationCount")
            if (
              !lastSnapshotVerification.get.checkpointBlocks.map {
                dao.checkpointService.get
              }.forall(_.exists(_.checkpointBlock.nonEmpty))
            ) {
              dao.metricsManager ! IncrementMetric("snapshotCBVerificationFailed")
            } else {
              dao.metricsManager ! IncrementMetric("snapshotCBVerificationCount")
            }

          }
        }
*/

          lastSnapshotHeight = nextHeightInterval
          snapshot = nextSnapshot
          acceptedCBSinceSnapshot = acceptedCBSinceSnapshot.filterNot(hashesForNextSnapshot.contains)
          dao.metricsManager ! UpdateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
          dao.metricsManager ! UpdateMetric("lastSnapshotHeight", lastSnapshotHeight.toString)
          dao.metricsManager ! UpdateMetric("nextSnapshotHeight", (lastSnapshotHeight + dao.processingConfig.snapshotHeightInterval).toString)
        }
      }
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
  def accept(checkpointCacheData: CheckpointCacheData)(implicit dao: DAO): Unit = this.synchronized {

    tryWithMetric(acceptCheckpoint(checkpointCacheData), "acceptCheckpoint")

    def reuseTips: Boolean = thresholdMetCheckpoints.size < dao.maxWidth

    checkpointCacheData.checkpointBlock.foreach { checkpointBlock =>

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

}


// TODO: Use atomicReference increment pattern instead of synchronized

class StorageService[T](size: Int = 50000) {


  val lruCache: MutableLRUCache[String, T] = {
    import com.twitter.storehaus.cache._
    MutableLRUCache[String, T](size)
  }

  // val actualDatastore = ... .update

  // val mutexStore = TrieMap[String, AtomicUpdater]

  // val mutexKeyCache = mutable.Queue()

  // if mutexKeyCache > size :
  // poll and remove from mutexStore?
  // mutexStore.getOrElseUpdate(hash)
  // Map[Address, AtomicUpdater] // computeIfAbsent getOrElseUpdate
/*  class AtomicUpdater {
    def update(
                key: String,
                updateFunc: T => T,
                empty: => T
              ): T =
      this.synchronized{
        val data = get(key).map {updateFunc}.getOrElse(empty)
        put(key, data)
        data
      }
  }*/

  def delete(keys: Set[String]) = this.synchronized{
    lruCache.multiRemove(keys)
  }

  def get(key: String): Option[T] = this.synchronized{
    lruCache.get(key)
  }

  def put(key: String, cache: T): Unit = this.synchronized{
    lruCache.+=((key, cache))
  }

  def update(
              key: String,
              updateFunc: T => T,
              empty: => T
            ): T =
    this.synchronized{
      val data = get(key).map {updateFunc}.getOrElse(empty)
      put(key, data)
      data
    }


}


// TODO: Make separate one for acceptedCheckpoints vs nonresolved etc.
class CheckpointService(size: Int = 50000) extends StorageService[CheckpointCacheData](size)
class TransactionService(size: Int = 50000) extends StorageService[TransactionCacheData](size)
class AddressService(size: Int = 50000) extends StorageService[AddressCacheData](size)

trait EdgeDAO {

  var processingConfig = ProcessingConfig()


  val checkpointService = new CheckpointService(processingConfig.checkpointLRUMaxSize)
  val transactionService = new TransactionService(processingConfig.transactionLRUMaxSize)
  val addressService = new AddressService(processingConfig.addressLRUMaxSize)

  val threadSafeTXMemPool = new ThreadSafeTXMemPool()
  val threadSafeTipService = new ThreadSafeTipService()

  var genesisObservation: Option[GenesisObservation] = None
  def maxWidth: Int = processingConfig.maxWidth
  def minCheckpointFormationThreshold: Int = processingConfig.minCheckpointFormationThreshold
  def minCBSignatureThreshold: Int = processingConfig.numFacilitatorPeers


  val resolveNotifierCallbacks: TrieMap[String, Seq[CheckpointBlock]] = TrieMap()

  val edgeExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

 // val peerAPIExecutionContext: ExecutionContextExecutor =
 //   ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  val apiClientExecutionContext: ExecutionContextExecutor = edgeExecutionContext
  //  ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  val signatureExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  val finishedExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  // Temporary to get peer data for tx hash partitioning
  @volatile var peerInfo: Map[Id, PeerData] = Map()


}
