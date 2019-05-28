package org.constellation.primitives

import java.util.concurrent.{Executors, Semaphore, TimeUnit}

import akka.util.Timeout
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.constellation.consensus._
import org.constellation.primitives.Schema._
import org.constellation.storage._
import org.constellation.util.Metrics
import org.constellation.{ConfigUtil, DAO, NodeConfig, ProcessingConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

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
case class PendingAcceptance(cb: CheckpointBlock)
      extends Exception(s"CheckpointBlock: ${cb.baseHash} is already pending acceptance phase.")
case class CheckpointAcceptBlockAlreadyStored(cb: CheckpointBlock)
      extends Exception(s"CheckpointBlock: ${cb.baseHash} is already stored.")

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
        addressCacheData = dao.addressService.toMap().unsafeRunSync(),
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
          dao.addressService.put(k, v).unsafeRunSync()
      }

      (latestSnapshotInfo.snapshotCache ++ latestSnapshotInfo.acceptedCBSinceSnapshotCache)
        .foreach { h =>
          // TODO: wkoszycki revisit it should call accept method instead
          dao.checkpointService.memPool.put(h.checkpointBlock.get.baseHash, h).unsafeRunSync()
          h.checkpointBlock.get.storeSOE().unsafeRunSync()
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

      val facilMap = dao.readyPeers(NodeType.Full).unsafeRunSync().filter {
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

  def accept(checkpoint: CheckpointCache)(implicit dao: DAO): IO[Unit] = {

    val acceptCheckpoint: IO[Unit] = checkpoint.checkpointBlock match {
      case None => IO.raiseError(MissingCheckpointBlockException)

      case Some(cb) if dao.checkpointService.contains(cb.baseHash) =>
        for {
        _ <- dao.metrics.incrementMetricAsync("checkpointAcceptBlockAlreadyStored")
        _ <- IO.raiseError(CheckpointAcceptBlockAlreadyStored(cb))
      } yield ()

      case Some(cb) =>
        for {
          _ <- syncPending(cb)
          conflicts <- CheckpointBlockValidatorNel.containsAlreadyAcceptedTx(cb)
          _ <- conflicts match {
            case Nil => IO.unit
            case _ =>
              concurrentTipService
                .putConflicting(cb.baseHash, cb)
                .flatMap(_ => IO.raiseError(TipConflictException(cb, conflicts)))
          }
          _ <- cb.storeSOE()
          maybeHeight <- calculateHeight(checkpoint)
          metricKey = if (maybeHeight.isEmpty) Metrics.heightEmpty else Metrics.heightNonEmpty
          _ <- dao.metrics.incrementMetricAsync(metricKey)
          _ <- dao.checkpointService.memPool.put(cb.baseHash, checkpoint.copy(height = maybeHeight))
          _ <- IO.delay(dao.recentBlockTracker.put(checkpoint.copy(height = maybeHeight)))
          _ <- acceptMessages(cb)
          _ <- acceptTransactions(cb)
          _ <- IO { logger.info(s"[${dao.id.short}] Accept checkpoint=${cb.baseHash}]") }
          _ <- concurrentTipService.update(cb)
          _ <- updateAcceptedCBSinceSnapshot(cb)
          _ <- IO.shift *> dao.metrics.incrementMetricAsync(Metrics.checkpointAccepted)
          _ <- dao.checkpointService.pendingAcceptance.remove(cb.baseHash)
        } yield ()

    }
    acceptCheckpoint.recoverWith {
      case err =>
        IO.shift *> dao.metrics.incrementMetricAsync("acceptCheckpoint_failure")
        IO.raiseError(err) //propagate to upper levels
    }
  }

  private def syncPending(cb: CheckpointBlock)(implicit dao: DAO): IO[Unit] = {
    IO {
      dao.checkpointService.pendingAcceptance.synchronized {
        if (dao.checkpointService.pendingAcceptance.contains(
          cb.baseHash
        ).unsafeRunSync()) {
          throw PendingAcceptance(cb)
        } else {
          dao.checkpointService.pendingAcceptance
            .put(cb.baseHash, cb).unsafeRunSync()
        }
      }
    }
  }

  private def updateAcceptedCBSinceSnapshot(cb: CheckpointBlock)(implicit dao: DAO): IO[Unit] = {
    IO {
      acceptedCBSinceSnapshot.synchronized {
        if (acceptedCBSinceSnapshot.contains(cb.baseHash)) {
          dao.metrics.incrementMetric(
            "checkpointAcceptedButAlreadyInAcceptedCBSinceSnapshot"
          )
        } else {
          acceptedCBSinceSnapshot = acceptedCBSinceSnapshot :+ cb.baseHash
          dao.metrics.updateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size)
        }
      }
    }
  }

  private def calculateHeight(
    checkpointCacheData: CheckpointCache
  )(implicit dao: DAO): IO[Option[Height]] = {
    IO {
      checkpointCacheData.checkpointBlock.flatMap { cb =>
        cb.calculateHeight() match {
          case None       => checkpointCacheData.height
          case calculated => calculated
        }
      }
    }
  }

  private def acceptMessages(cb: CheckpointBlock)(implicit dao: DAO): IO[List[Unit]] = {
    cb.messages
      .map { m =>
        val channelMessageMetadata = ChannelMessageMetadata(m, Some(cb.baseHash))
        val messageUpdate =
          if (m.signedMessageData.data.previousMessageHash != Genesis.CoinBaseHash) {
            for {
              _ <- dao.messageService.memPool.put(
                m.signedMessageData.data.channelId,
                channelMessageMetadata
              )
              _ <- dao.channelService.update(
                m.signedMessageData.hash, { cmd =>
                  val slicedMessages = cmd.last25MessageHashes.slice(0, 25)
                  cmd.copy(
                    totalNumMessages = cmd.totalNumMessages + 1,
                    last25MessageHashes = Seq(m.signedMessageData.hash) ++ slicedMessages
                  )
                }
              )
            } yield ()
          } else { // Unsafe json extract
            dao.channelService.put(
              m.signedMessageData.hash,
              ChannelMetadata(
                m.signedMessageData.data.message.x[ChannelOpen],
                channelMessageMetadata
              )
            )
          }

        for {
          _ <- messageUpdate
          _ <- dao.messageService.memPool
            .put(m.signedMessageData.hash, channelMessageMetadata)
          _ <- dao.metrics.incrementMetricAsync("messageAccepted")
        } yield ()
      }
      .toList
      .sequence
  }

  private def acceptTransactions(cb: CheckpointBlock)(implicit dao: DAO): IO[Unit] = {
    def toCacheData(tx: Transaction) = TransactionCacheData(
      tx,
      valid = true,
      inMemPool = false,
      inDAG = true,
      Map(cb.baseHash -> true),
      resolved = true,
      cbBaseHash = Some(cb.baseHash)
    )

    val insertTX =
      cb.transactions
        .toList
        .map(tx ⇒ (tx, toCacheData(tx)))
        .traverse {
          case (tx, txMetadata) =>
            dao.transactionService.accept(txMetadata) *>
              dao.addressService.transfer(tx)
        }
        .void

    IO { logger.info(s"Accepting transactions ${cb.transactions.size}") } >> insertTX
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

  var transactionService: TransactionService[IO] = _
  var checkpointService: CheckpointService = _
  var snapshotService: SnapshotService = _
  var addressService: AddressService = _

  val notificationService = new NotificationService()
  val messageService : MessageService
  val channelService = new ChannelService()
  val soeService = new SOEService(ConfigUtil.getOrElse("constellation.cache.soe-mem-pool-eviction-minutes", 15))

  val recentBlockTracker = new RecentDataTracker[CheckpointCache](200)

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

  val apiClientExecutionContext: ExecutionContextExecutor =
    edgeExecutionContext

  val signatureExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))

  val finishedExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))


  def pullMessages(minimumCount: Int): Option[Seq[ChannelMessage]] = {
    threadSafeMessageMemPool.pull(minimumCount)
  }

}
