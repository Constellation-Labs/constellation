package org.constellation.storage

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import cats.effect.{IO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.Logger
import constellation.tryWithMetric
import org.constellation.consensus.{Snapshot, SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Schema.{CheckpointCache, NodeState, NodeType}
import org.constellation.primitives._
import org.constellation.util.Metrics
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
          dao.metrics.incrementMetricAsync[IO](Metrics.checkpointAccepted) *>
          h.checkpointBlock.get
            .transactions.toList
            .map(_ => dao.metrics.incrementMetricAsync[IO]("transactionAccepted")).sequence)
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

          val hashesForNextSnapshot = allblockCaches.flatMap(_.checkpointBlock.map(_.baseHash)).sorted
          val nextSnapshot = Snapshot(snapshot.hash, hashesForNextSnapshot)

          // TODO: Make this a future and have it not break the unit test
          // Also make the db puts blocking, may help for different issue
          if (snapshot != Snapshot.snapshotZero) {

            dao.metrics.incrementMetric(Metrics.snapshotCount)
            logger.info("Performing snapshot")

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
                  dao.checkpointService.applySnapshot(snapshot.checkpointBlocks.toList) *>
                  IO { totalNumCBsInShapshots += snapshot.checkpointBlocks.size } *>
                  dao.metrics.updateMetricAsync[IO]("totalNumCBsInShapshots",
                                                totalNumCBsInShapshots.toString) *>
                  dao.metrics.updateMetricAsync[IO](Metrics.lastSnapshotHash, snapshot.hash)
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

  def updateAcceptedCBSinceSnapshot(cb: CheckpointBlock)(implicit dao: DAO): IO[Unit] = {
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

}
