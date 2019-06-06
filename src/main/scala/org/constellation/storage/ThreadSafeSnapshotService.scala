package org.constellation.storage

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.Logger
import constellation.tryWithMetric
import org.constellation.consensus.{Snapshot, SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Schema.{CheckpointCache, Height, NodeState, NodeType}
import org.constellation.primitives._
import org.constellation.util.Metrics
import constellation._
import org.constellation.DAO

import scala.util.Try

class ThreadSafeSnapshotService(concurrentTipService: ConcurrentTipService) {

  implicit val timeout: Timeout = Timeout(15, TimeUnit.SECONDS)
  val logger = Logger("ThreadSafeSnapshotService")
  var acceptedCBSinceSnapshot: Seq[String] = Seq()
  var totalNumCBsInShapshots = 0L
  // TODO: Read from lastSnapshot in DB optionally, assign elsewhere
  var lastSnapshotHeight = 0
  var syncBuffer: Seq[CheckpointCache] = Seq()

  // ONLY TO BE USED BY DOWNLOAD COMPLETION CALLER
  private var snapshot: Snapshot = Snapshot.snapshotZero

  def getSnapshotInfo()(implicit dao: DAO): SnapshotInfo = this.synchronized(
    SnapshotInfo(
      snapshot,
      acceptedCBSinceSnapshot,
      lastSnapshotHeight = lastSnapshotHeight,
      snapshotHashes = dao.snapshotHashes,
      addressCacheData = dao.addressService.toMap().unsafeRunSync(),
      tips = concurrentTipService.toMap,
      snapshotCache = snapshot.checkpointBlocks.flatMap {
        dao.checkpointService.fullData(_).unsafeRunSync()
      }
    )
  )

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
        (h.checkpointBlock.get.storeSOE() *>
          dao.checkpointService.memPool.put(h.checkpointBlock.get.baseHash, h) *>
          dao.metrics.incrementMetricAsync(Metrics.checkpointAccepted) *>
          h.checkpointBlock.get
            .transactions.toList
            .map(_ => dao.metrics.incrementMetricAsync("transactionAccepted")).sequence)
          .unsafeRunSync()
      }

    dao.metrics.updateMetric(
      "acceptCBCacheMatchesAcceptedSize",
      (latestSnapshotInfo.acceptedCBSinceSnapshot.size == latestSnapshotInfo.acceptedCBSinceSnapshotCache.size).toString
    )

  }

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
        dao.metrics.incrementMetric("snapshotHeightIntervalConditionNotMet")
      } else {
        dao.metrics.incrementMetric("snapshotHeightIntervalConditionMet")

        val maybeDatas =
          acceptedCBSinceSnapshot.map(dao.checkpointService.fullData(_).unsafeRunSync())

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

          val hashesForNextSnapshot = blockCaches.flatMap(_.checkpointBlock.map(_.baseHash)).sorted
          val nextSnapshot = Snapshot(snapshot.hash, hashesForNextSnapshot)

          // TODO: Make this a future and have it not break the unit test
          // Also make the db puts blocking, may help for different issue
          if (snapshot != Snapshot.snapshotZero) {

            dao.metrics.incrementMetric(Metrics.snapshotCount)

            // Write snapshot to file
            tryWithMetric(
              {
                val maybeBlocks = snapshot.checkpointBlocks.map {
                  dao.checkpointService.fullData(_).unsafeRunSync()
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

            val cbData = snapshot.checkpointBlocks.map {
              dao.checkpointService.fullData(_).unsafeRunSync()
            }
            val cbs = cbData.toList.map(_.flatMap(_.checkpointBlock)).sequence
            val addresses =
              cbs.map(_.flatMap(_.transactions.toList.flatMap(t => List(t.src, t.dst))).toSet)

            dao.addressService
              .lockForSnapshot(
                addresses.get,
                IO(Snapshot.acceptSnapshot(snapshot)) *>
                  dao.snapshotService.midDb.put(snapshot.hash, snapshot) *>
                  dao.checkpointService.applySnapshot(snapshot.checkpointBlocks.toList) *>
                  IO { totalNumCBsInShapshots += snapshot.checkpointBlocks.size } *>
                  dao.metrics.updateMetricAsync("totalNumCBsInShapshots",
                                                totalNumCBsInShapshots.toString) *>
                  dao.metrics.updateMetricAsync(Metrics.lastSnapshotHash, snapshot.hash)
              )
              .unsafeRunSync()
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
          dao.metrics.updateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size)
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

      case Some(cb) if dao.checkpointService.contains(cb.baseHash).unsafeRunSync() =>
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
          _ <- if (maybeHeight.isEmpty)
            dao.metrics
              .incrementMetricAsync(Metrics.heightEmpty)
              .flatMap(_ => IO.raiseError(MissingHeightException(cb)))
          else IO.unit
          _ <- dao.checkpointService.memPool.put(cb.baseHash, checkpoint.copy(height = maybeHeight))
          _ <- IO.delay(dao.recentBlockTracker.put(checkpoint.copy(height = maybeHeight)))
          _ <- acceptMessages(cb)
          _ <- acceptTransactions(cb)
          _ <- IO { logger.debug(s"[${dao.id.short}] Accept checkpoint=${cb.baseHash}]") }
          _ <- concurrentTipService.update(cb)
          _ <- updateAcceptedCBSinceSnapshot(cb)
          _ <- IO.shift *> dao.metrics.incrementMetricAsync(Metrics.checkpointAccepted)
          _ <- dao.checkpointService.pendingAcceptance.remove(cb.baseHash)
        } yield ()

    }

    acceptCheckpoint.recoverWith {
      case err =>
        dao.metrics
          .incrementMetricAsync("acceptCheckpoint_failure")
          .flatMap(_ => IO.raiseError(err)) //propagate to upper levels
    }
  }

  private def syncPending(cb: CheckpointBlock)(implicit dao: DAO): IO[Unit] = {
    IO {
      dao.checkpointService.pendingAcceptance.synchronized {
        if (dao.checkpointService.pendingAcceptance
              .contains(
                cb.baseHash
              )
              .unsafeRunSync()) {
          throw PendingAcceptance(cb)
        } else {
          dao.checkpointService.pendingAcceptance
            .put(cb.baseHash, cb)
            .unsafeRunSync()
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
      cb.transactions.toList
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
