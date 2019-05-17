package org.constellation.primitives

import java.util.concurrent.{Executors, Semaphore, TimeUnit}

import akka.util.Timeout
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.constellation.consensus.EdgeProcessor.acceptCheckpoint
import org.constellation.consensus._
import org.constellation.primitives.Schema._
import org.constellation.primitives.storage._
import org.constellation.util.Metrics
import org.constellation.{DAO, NodeConfig, ProcessingConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success, Try}

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

class ThreadSafeMessageMemPool() extends StrictLogging {

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

  // TODO: Fix
  def pull(minCount: Int = 1): Option[Seq[ChannelMessage]] = this.synchronized {
    /*if (messages.size >= minCount) {
      val (left, right) = messages.splitAt(minCount)
      messages = right
      Some(left.flatten)
    } else None*/
    val flat = messages.flatten
    messages = Seq()
    if (flat.isEmpty) None
    else {
      logger.info(s"Pulled messages from mempool: ${flat.map {
        _.signedMessageData.hash
      }}")
      Some(flat)
    }
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
// TODO: wkoszycki this one is temporary till (#412 Flatten checkpointBlock in CheckpointCache) is finished
case object MissingCheckpointBlockException extends Exception("CheckpointBlock object is empty.")
case object CheckpointAcceptBlockAlreadyStored extends Exception("CheckpointBlock is already stored.")

class ThreadSafeSnapshotService(concurrentTipService: ConcurrentTipService) {

  implicit val timeout: Timeout = Timeout(15, TimeUnit.SECONDS)

  var acceptedCBSinceSnapshot: Seq[String] = Seq()
  private var snapshot: Snapshot = Snapshot.snapshotZero
  val logger = Logger("ThreadSafeSnapshotService")

  def getSnapshotInfo()(implicit dao: DAO): SnapshotInfo = this.synchronized(
      SnapshotInfo(
        snapshot,
        acceptedCBSinceSnapshot,
        lastSnapshotHeight = lastSnapshotHeight,
        snapshotHashes = dao.snapshotHashes,
        addressCacheData = dao.addressService.toMapSync(),
        tips = concurrentTipService.toMap,
        snapshotCache = snapshot.checkpointBlocks.flatMap { dao.checkpointService.getFullData }
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

      (latestSnapshotInfo.snapshotCache ++ latestSnapshotInfo.acceptedCBSinceSnapshotCache)
        .foreach { h =>
          dao.checkpointService.memPool.putSync(h.checkpointBlock.get.baseHash, h)
          h.checkpointBlock.get.storeSOE()
          dao.metrics.incrementMetric(Metrics.checkpointAccepted)
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

  var syncBuffer: Seq[CheckpointCache] = Seq()

  def syncBufferAccept(cb: CheckpointCache)(implicit dao: DAO): Unit = {
      syncBuffer :+= cb
      dao.metrics.updateMetric("syncBufferSize", syncBuffer.size.toString)
    }

  def attemptSnapshot()(implicit dao: DAO): Unit = this.synchronized {
      if (acceptedCBSinceSnapshot.size > dao.processingConfig.maxAcceptedCBHashesInMemory) {
        acceptedCBSinceSnapshot = acceptedCBSinceSnapshot.slice(0, 100)
        dao.metrics.incrementMetric("memoryExceeded_acceptedCBSinceSnapshot")
        dao.metrics.updateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
      }

      val facilMap = dao.readyPeersAsync(NodeType.Full).unsafeRunSync().filter {
        case (_, pd) =>
          // TODO: Is this still necessary?
          pd.peerMetadata.timeAdded < (System
            .currentTimeMillis() - dao.processingConfig.minPeerTimeAddedSeconds * 1000)
      }

      if (dao.nodeState == NodeState.Ready && acceptedCBSinceSnapshot.nonEmpty) {

        val minTipHeight = Try { concurrentTipService.getMinTipHeight() }.getOrElse(0L)
        dao.metrics.updateMetric("minTipHeight", minTipHeight.toString)

        val nextHeightInterval = lastSnapshotHeight + dao.processingConfig.snapshotHeightInterval

        val canSnapshot = minTipHeight > (nextHeightInterval + dao.processingConfig.snapshotHeightDelayInterval)
        if (!canSnapshot) {
          logger.info("Snapshot - height interval condition not met")
          dao.metrics.incrementMetric("snapshotHeightIntervalConditionNotMet")
        } else {
          dao.metrics.incrementMetric("snapshotHeightIntervalConditionMet")

          logger.info("--------- Snapshot - height interval condition met")

          val maybeDatas = acceptedCBSinceSnapshot.map(dao.checkpointService.getFullData)

          val blocksWithinHeightInterval = maybeDatas.filter {
            _.exists(_.height.exists { h =>
              h.min > lastSnapshotHeight && h.min <= nextHeightInterval
            })
          }

          if (blocksWithinHeightInterval.isEmpty) {
            dao.metrics.incrementMetric("snapshotNoBlocksWithinHeightInterval")
          } else {

            val allblockCaches = blocksWithinHeightInterval.map {
              _.get
            }
            val maybeConflictingTip =
              CheckpointBlockValidatorNel.detectInternalTipsConflict(allblockCaches)

            val blockCaches = if (maybeConflictingTip.isDefined) {
              val conflictingTip = maybeConflictingTip.get.checkpointBlock.get.baseHash
              logger.warn(
                s"Conflict tip detected while attempt to make snapshot tipHash: $conflictingTip"
              )
              concurrentTipService.markAsConflict(conflictingTip)(dao.metrics)
              acceptedCBSinceSnapshot = acceptedCBSinceSnapshot.filterNot(_ == conflictingTip)
              allblockCaches.filterNot(_ == maybeConflictingTip.get)
            } else {
              allblockCaches
            }

            val hashesForNextSnapshot = blockCaches.flatMap {
              _.checkpointBlock.map(_.baseHash)
            }.sorted
            val nextSnapshot = Snapshot(snapshot.hash, hashesForNextSnapshot)

            // TODO: Make this a future and have it not break the unit test
            // Also make the db puts blocking, may help for different issue
            if (snapshot != Snapshot.snapshotZero) {
              logger.info("--------- Snapshot - not zero snapshot")

              dao.metrics.incrementMetric(Metrics.snapshotCount)

              // Write snapshot to file
              tryWithMetric(
                {
                  val maybeBlocks = snapshot.checkpointBlocks.map {
                    dao.checkpointService.getFullData
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

              val cbData = snapshot.checkpointBlocks.map { dao.checkpointService.getFullData }
              val cbs = cbData.toList.map(_.flatMap(_.checkpointBlock)).sequence
              val addresses =
                cbs.map(_.flatMap(_.transactions.toList.flatMap(t => List(t.src, t.dst))).toSet)

              logger.info("--------- Snapshot - just before lock for snapshot")

              dao.addressService
                .lockForSnapshot(
                  addresses.get,
                  IO {
                    logger.info("--------- Snapshot - lock for snapshot - acquired")
                    Snapshot.acceptSnapshot(snapshot)
                    dao.snapshotService.midDb.put(snapshot.hash, snapshot)
                    dao.checkpointService.memPool.remove(snapshot.checkpointBlocks.toSet)

                    totalNumCBsInShapshots += snapshot.checkpointBlocks.size
                    dao.metrics.updateMetric("totalNumCBsInShapshots",
                                             totalNumCBsInShapshots.toString)
                    dao.metrics.updateMetric(Metrics.lastSnapshotHash, snapshot.hash)
                  }
                )
                .unsafeRunSync()
            } else {
              logger.info("--------- Snapshot - it's a zero snapshot")
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
            dao.metrics.updateMetric("acceptedCBSinceSnapshot",
                                     acceptedCBSinceSnapshot.size)
            dao.metrics.updateMetric("lastSnapshotHeight", lastSnapshotHeight)
            dao.metrics.updateMetric(
              "nextSnapshotHeight",
              lastSnapshotHeight + dao.processingConfig.snapshotHeightInterval
            )
          }
        }
      }
    }

  implicit val shift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

  def accept(checkpoint: CheckpointCache)(implicit dao: DAO): Try[Unit] = {

    checkpoint.checkpointBlock match {
      case None => Failure(MissingCheckpointBlockException)

      case Some(cb) if dao.checkpointService.contains(cb.baseHash) =>
        dao.metrics.incrementMetric("checkpointAcceptBlockAlreadyStored")
        Failure(CheckpointAcceptBlockAlreadyStored)

      case Some(cb) =>
        tryWithMetric(acceptCheckpoint(checkpoint), "acceptCheckpoint")
        concurrentTipService.update(cb).unsafeRunSync() match {
          case Right(_) =>
            acceptedCBSinceSnapshot.synchronized {
              if (acceptedCBSinceSnapshot.contains(cb.baseHash)) {
                (IO.shift *> dao.metrics.incrementMetricAsync(
                  "checkpointAcceptedButAlreadyInAcceptedCBSinceSnapshot"
                )).unsafeRunAsyncAndForget()
              } else {
                acceptedCBSinceSnapshot = acceptedCBSinceSnapshot :+ cb.baseHash
                dao.metrics.updateMetric("acceptedCBSinceSnapshot",
                  acceptedCBSinceSnapshot.size)
              }
            }
            Success(())
          case Left(err) =>
            logger.error(s"Unable to accept checkpoint due to: ${err.getMessage}", err)

            (IO.shift *> dao.metrics.incrementMetricAsync("acceptedCBSinceSnapshotFailure"))
              .unsafeRunAsyncAndForget()
            Failure(err)

        }
    }

  }
}

trait EdgeDAO {

  var metrics: Metrics

  @volatile var nodeConfig: NodeConfig

  def processingConfig: ProcessingConfig = nodeConfig.processingConfig

  private val blockFormationLock: Any = new Object()

  private[this] var _blockFormationInProgress: Boolean = false

  def blockFormationInProgress: Boolean = blockFormationLock.synchronized {
    _blockFormationInProgress
  }

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
  var acceptedTransactionService: AcceptedTransactionService = _
  var addressService: AddressService = _

  val notificationService = new NotificationService()
  val messageService : MessageService
  val channelService = new ChannelService()
  val soeService = new SOEService()

  val recentBlockTracker = new RecentDataTracker[CheckpointCache](200)

  val threadSafeTXMemPool = new ThreadSafeTXMemPool()

  val threadSafeMessageMemPool = new ThreadSafeMessageMemPool()

  var genesisBlock: Option[CheckpointBlock] = None
  var genesisObservation: Option[GenesisObservation] = None

  def maxWidth: Int = processingConfig.maxWidth

  def minCheckpointFormationThreshold: Int = processingConfig.minCheckpointFormationThreshold
  def maxTXInBlock: Int = processingConfig.maxTXInBlock

  def minCBSignatureThreshold: Int = processingConfig.numFacilitatorPeers

  val resolveNotifierCallbacks: TrieMap[String, Seq[CheckpointBlock]] = TrieMap()

  val edgeExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))

//  val snapshotExecutionContext: ExecutionContextExecutor =
//    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))
//
//   val peerAPIExecutionContext: ExecutionContextExecutor =
//     ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  val apiClientExecutionContext: ExecutionContextExecutor =
    edgeExecutionContext
//    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  val signatureExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))

  val finishedExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))

  def pullTransactions(
    minimumCount: Int = minCheckpointFormationThreshold
  ): Option[Seq[Transaction]] = {
    val txs = threadSafeTXMemPool.pull(minimumCount)

    txs.foreach(_.foreach(_ =>
      metrics.incrementMetric("transactionPull")
    ))

    txs
  }

  def pullMessages(minimumCount: Int): Option[Seq[ChannelMessage]] = {
    threadSafeMessageMemPool.pull(minimumCount)
  }

}
