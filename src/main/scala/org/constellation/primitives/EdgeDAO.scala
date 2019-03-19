package org.constellation.primitives

import java.util.concurrent.{Executors, Semaphore, TimeUnit}

import cats.implicits._
import akka.util.Timeout
import org.constellation.consensus.EdgeProcessor.acceptCheckpoint
import org.constellation.consensus._
import org.constellation.primitives.Schema._
import org.constellation.primitives.storage._
import org.constellation.util.Metrics
import org.constellation.{DAO, NodeConfig, ProcessingConfig}
import org.constellation.primitives.storage.{SnapshotsMidDbStorage, _}
import org.constellation.{DAO, ProcessingConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class ThreadSafeTXMemPool() {

  private var transactions = Seq[Transaction]()

  def pull(minCount: Int): Option[Seq[Transaction]] = this.synchronized {
    if (transactions.size > minCount) {
      val (left, right) = transactions.splitAt(minCount)
      transactions = right
      Some(left)
    } else None
  }

  def pullUpTo(minCount: Int): Seq[Transaction] = this.synchronized {
    val (left, right) = transactions.splitAt(minCount)
    transactions = right
    left
  }

  def batchPutDebug(txs: Seq[Transaction]): Boolean = this.synchronized {
    transactions ++= txs
    true
  }

  def put(transaction: Transaction, overrideLimit: Boolean = false)(implicit dao: DAO): Boolean =
    this.synchronized {
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

class ThreadSafeMessageMemPool() {

  private var messages = Seq[Seq[ChannelMessage]]()

  val activeChannels: TrieMap[String, Semaphore] = TrieMap()

  val selfChannelNameToGenesisMessage: TrieMap[String, ChannelMessage] = TrieMap()
  val selfChannelIdToName: TrieMap[String, String] = TrieMap()

  val messageHashToSendRequest: TrieMap[String, ChannelSendRequest] = TrieMap()

  def release(messages: Seq[ChannelMessage]): Unit = {
    messages.foreach { m =>
      activeChannels.get(m.signedMessageData.data.channelId).foreach {
        _.release()
      }

    }
  }

  def pull(minCount: Int): Option[Seq[ChannelMessage]] = this.synchronized {
    if (messages.size > minCount) {
      val (left, right) = messages.splitAt(minCount)
      messages = right
      Some(left.flatten)
    } else None
  }

  def batchPutDebug(messagesToAdd: Seq[ChannelMessage]): Boolean = this.synchronized {
    //messages ++= messagesToAdd
    true
  }

  def put(message: Seq[ChannelMessage],
          overrideLimit: Boolean = false)(implicit dao: DAO): Boolean = this.synchronized {
    val notContained = !messages.contains(message)

    if (notContained) {
      if (overrideLimit) {
        // Prepend in front to process user TX first before random ones
        messages = Seq(message) ++ messages

      } else if (messages.size < dao.processingConfig.maxMemPoolSize) {
        messages :+= message
      }
    }
    notContained
  }

  def unsafeCount: Int = messages.size

}

import constellation._

class ThreadSafeSnapshotService(concurrentTipService: ConcurrentTipService) {

  implicit val timeout: Timeout = Timeout(15, TimeUnit.SECONDS)

  var acceptedCBSinceSnapshot: Seq[String] = Seq()
  private var snapshot: Snapshot = Snapshot.snapshotZero

  def tips: Map[String, TipData] = concurrentTipService.toMap

  def getSnapshotInfo()(implicit dao: DAO): SnapshotInfo = this.synchronized(
    SnapshotInfo(
      snapshot,
      acceptedCBSinceSnapshot,
      lastSnapshotHeight = lastSnapshotHeight,
      snapshotHashes = dao.snapshotHashes,
      addressCacheData = dao.addressService.toMapSync(),
      tips = concurrentTipService.toMap,
      snapshotCache = snapshot.checkpointBlocks.flatMap { dao.checkpointService.get }
    )
  )

  var totalNumCBsInShapshots = 0L

  // ONLY TO BE USED BY DOWNLOAD COMPLETION CALLER

  def setSnapshot(latestSnapshotInfo: SnapshotInfo)(implicit dao: DAO): Unit = this.synchronized {
    snapshot = latestSnapshotInfo.snapshot
    lastSnapshotHeight = latestSnapshotInfo.lastSnapshotHeight
    concurrentTipService.set(latestSnapshotInfo.tips)

    // Below may not be necessary, just a sanity check
    acceptedCBSinceSnapshot = latestSnapshotInfo.acceptedCBSinceSnapshot
    latestSnapshotInfo.addressCacheData.foreach {
      case (k, v) =>
        dao.addressService.putSync(k, v)
    }

    latestSnapshotInfo.snapshotCache.foreach { h =>
      dao.metrics.incrementMetric("checkpointAccepted")
      dao.checkpointService.memPool.put(h.checkpointBlock.get.baseHash, h)
      h.checkpointBlock.get.storeSOE()
      h.checkpointBlock.get.transactions.foreach { _ =>
        dao.metrics.incrementMetric("transactionAccepted")
      }
    }

    latestSnapshotInfo.acceptedCBSinceSnapshotCache.foreach { h =>
      dao.checkpointService.memPool.put(h.checkpointBlock.get.baseHash, h)
      h.checkpointBlock.get.storeSOE()
      dao.metrics.incrementMetric("checkpointAccepted")
      h.checkpointBlock.get.transactions.foreach { _ =>
        dao.metrics.incrementMetric("transactionAccepted")
      }
    }

    dao.metrics.updateMetric(
      "acceptCBCacheMatchesAcceptedSize",
      (latestSnapshotInfo.acceptedCBSinceSnapshot.size == latestSnapshotInfo.acceptedCBSinceSnapshotCache.size).toString
    )

  }

  // TODO: Read from lastSnapshot in DB optionally, assign elsewhere
  var lastSnapshotHeight = 0

  var syncBuffer: Seq[CheckpointCacheData] = Seq()

  def syncBufferAccept(cb: CheckpointCacheData)(implicit dao: DAO): Unit = {
    syncBuffer :+= cb
    dao.metrics.updateMetric("syncBufferSize", syncBuffer.size.toString)
  }

  def attemptSnapshot()(implicit dao: DAO): Unit = this.synchronized {

    if (acceptedCBSinceSnapshot.size > dao.processingConfig.maxAcceptedCBHashesInMemory) {
      acceptedCBSinceSnapshot = acceptedCBSinceSnapshot.slice(0, 100)
      dao.metrics.incrementMetric("memoryExceeded_acceptedCBSinceSnapshot")
      dao.metrics.updateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
    }

    val peerIds = dao.peerInfo //(dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq
    val facilMap = peerIds.filter {
      case (_, pd) =>
        pd.peerMetadata.timeAdded < (System
          .currentTimeMillis() - dao.processingConfig.minPeerTimeAddedSeconds * 1000) && pd.peerMetadata.nodeState == NodeState.Ready
    }

    if (dao.nodeState == NodeState.Ready && acceptedCBSinceSnapshot.nonEmpty) {

      val minTipHeight = concurrentTipService.getMinTipHeight()
      dao.metrics.updateMetric("minTipHeight", minTipHeight.toString)

      val nextHeightInterval = lastSnapshotHeight + dao.processingConfig.snapshotHeightInterval

      val canSnapshot = minTipHeight > (nextHeightInterval + dao.processingConfig.snapshotHeightDelayInterval)
      if (!canSnapshot) {
        dao.metrics.incrementMetric("snapshotHeightIntervalConditionNotMet")
      } else {

        val maybeDatas = acceptedCBSinceSnapshot.map(dao.checkpointService.get)

        val blocksWithinHeightInterval = maybeDatas.filter {
          _.exists(_.height.exists { h =>
            h.min > lastSnapshotHeight && h.min <= nextHeightInterval
          })
        }

        if (blocksWithinHeightInterval.isEmpty) {
          dao.metrics.incrementMetric("snapshotNoBlocksWithinHeightInterval")
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
            dao.metrics.incrementMetric("snapshotCount")

            // Write snapshot to file
            tryWithMetric(
              {
                val maybeBlocks = snapshot.checkpointBlocks.map {
                  dao.checkpointService.get
                }
                if (maybeBlocks.exists(_.exists(_.checkpointBlock.isEmpty))) {
                  // TODO : This should never happen, if it does we need to reset the node state and redownload
                  dao.metrics.incrementMetric("snapshotWriteToDiskMissingData")
                }
                val flatten = maybeBlocks.flatten.sortBy(_.checkpointBlock.map {
                  _.baseHash
                })
                Snapshot.writeSnapshot(StoredSnapshot(snapshot, flatten))
                // dao.dbActor.kvdb.put("latestSnapshot", snapshot)
              },
              "snapshotWriteToDisk"
            )

            Snapshot.acceptSnapshot(snapshot)
            dao.snapshotService.midDb.put(snapshot.hash, snapshot)
            dao.checkpointService.memPool.remove(snapshot.checkpointBlocks.toSet)

            totalNumCBsInShapshots += snapshot.checkpointBlocks.size
            dao.metrics.updateMetric("totalNumCBsInShapshots", totalNumCBsInShapshots.toString)
            dao.metrics.updateMetric("lastSnapshotHash", snapshot.hash)
          }

          // TODO: Verify from file
          /*
        if (snapshot.lastSnapshot != Snapshot.snapshotZeroHash && snapshot.lastSnapshot != "") {

          val lastSnapshotVerification = File(dao.snapshotPath, snapshot.lastSnapshot).read
          if (lastSnapshotVerification.isEmpty) {
            dao.metrics.incrementMetric("snapshotVerificationFailed")
          } else {
            dao.metrics.incrementMetric("snapshotVerificationCount")
            if (
              !lastSnapshotVerification.get.checkpointBlocks.map {
                dao.checkpointService.memPool.getSync
              }.forall(_.exists(_.checkpointBlock.nonEmpty))
            ) {
              dao.metrics.incrementMetric("snapshotCBVerificationFailed")
            } else {
              dao.metrics.incrementMetric("snapshotCBVerificationCount")
            }

          }
        }
           */

          lastSnapshotHeight = nextHeightInterval
          snapshot = nextSnapshot
          acceptedCBSinceSnapshot =
            acceptedCBSinceSnapshot.filterNot(hashesForNextSnapshot.contains)
          dao.metrics.updateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
          dao.metrics.updateMetric("lastSnapshotHeight", lastSnapshotHeight.toString)
          dao.metrics.updateMetric(
            "nextSnapshotHeight",
            (lastSnapshotHeight + dao.processingConfig.snapshotHeightInterval).toString
          )
        }
      }
    }
  }

  // TODO: Synchronize only on values modified by this, same for other functions

  def accept(checkpointCacheData: CheckpointCacheData)(implicit dao: DAO): Unit =
    this.synchronized {

      if (dao.checkpointService.contains(
            checkpointCacheData.checkpointBlock
              .map {
                _.baseHash
              }
              .getOrElse("")
          )) {

        dao.metrics.incrementMetric("checkpointAcceptBlockAlreadyStored")

      } else {

        tryWithMetric(acceptCheckpoint(checkpointCacheData), "acceptCheckpoint")
        checkpointCacheData.checkpointBlock.foreach { checkpointBlock =>
          concurrentTipService.update(checkpointBlock)
          if (acceptedCBSinceSnapshot.contains(checkpointBlock.baseHash)) {
            dao.metrics.incrementMetric("checkpointAcceptedButAlreadyInAcceptedCBSinceSnapshot")
          } else {
            acceptedCBSinceSnapshot = acceptedCBSinceSnapshot :+ checkpointBlock.baseHash
            dao.metrics.updateMetric("acceptedCBSinceSnapshot",
                                     acceptedCBSinceSnapshot.size.toString)
          }
        }

      }
    }

}

trait EdgeDAO {

  var metrics: Metrics

  @volatile var nodeConfig : NodeConfig

  def processingConfig: ProcessingConfig = nodeConfig.processingConfig

  private val blockFormationLock: Any = new Object()

  private[this] var _blockFormationInProgress: Boolean = false

  def blockFormationInProgress: Boolean = blockFormationLock.synchronized { _blockFormationInProgress }

  def blockFormationInProgress_=(value: Boolean): Unit = blockFormationLock.synchronized {
    _blockFormationInProgress = value
    metrics.updateMetric("blockFormationInProgress", blockFormationInProgress.toString)
  }

  // TODO: Put on Id keyed datastore (address? potentially) with other metadata
  val publicReputation: TrieMap[Id, Double] = TrieMap()
  val secretReputation: TrieMap[Id, Double] = TrieMap()

  val otherNodeScores: TrieMap[Id, TrieMap[Id, Double]] = TrieMap()

  var transactionService: TransactionService = _
  var checkpointService: CheckpointService = _
  var snapshotService: SnapshotService = _

  val acceptedTransactionService = new AcceptedTransactionService(
    5000 //processingConfig.transactionLRUMaxSize
  )

  val addressService = new AddressService(
    5000
    // processingConfig.addressLRUMaxSize
  )
  val messageService = new MessageService()
  val channelService = new ChannelService()
  val soeService = new SOEService()

  val recentBlockTracker = new RecentDataTracker[CheckpointCacheData](200)

  val threadSafeTXMemPool = new ThreadSafeTXMemPool()
  lazy val concurrentTipService: ConcurrentTipService = new TrieBasedTipService(processingConfig.maxActiveTipsAllowedInMemory,
                                                     processingConfig.maxWidth,
                                                     processingConfig.numFacilitatorPeers,
                                                     processingConfig.minPeerTimeAddedSeconds)

  val threadSafeMessageMemPool = new ThreadSafeMessageMemPool()
  lazy val threadSafeSnapshotService = new ThreadSafeSnapshotService(concurrentTipService)

  var genesisBlock: Option[CheckpointBlock] = None
  var genesisObservation: Option[GenesisObservation] = None

  def maxWidth: Int = processingConfig.maxWidth

  def minCheckpointFormationThreshold: Int = processingConfig.minCheckpointFormationThreshold
  def maxTXInBlock: Int = processingConfig.maxTXInBlock

  def minCBSignatureThreshold: Int = processingConfig.numFacilitatorPeers

  val resolveNotifierCallbacks: TrieMap[String, Seq[CheckpointBlock]] = TrieMap()

  val edgeExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))

  // val peerAPIExecutionContext: ExecutionContextExecutor =
  //   ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  val apiClientExecutionContext: ExecutionContextExecutor = edgeExecutionContext
  //  ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  val signatureExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))

  val finishedExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))

  // Temporary to get peer data for tx hash partitioning
  @volatile var peerInfo: Map[Id, PeerData] = Map()

  def readyPeers: Map[Id, PeerData] =
    peerInfo.filter(_._2.peerMetadata.nodeState == NodeState.Ready)

  def readyFacilitators(): Map[Id, PeerData] = peerInfo.filter {
    case (_, pd) =>
      pd.peerMetadata.timeAdded < (System
        .currentTimeMillis() - processingConfig.minPeerTimeAddedSeconds * 1000) && pd.peerMetadata.nodeState == NodeState.Ready
  }

  def pullTransactions(minimumCount: Int = minCheckpointFormationThreshold): Option[Seq[Transaction]] =  {
    threadSafeTXMemPool.pull(minimumCount)
  }

  def pullMessages(minimumCount: Int): Option[Seq[ChannelMessage]] = {
    threadSafeMessageMemPool.pull(minimumCount)
  }

}
