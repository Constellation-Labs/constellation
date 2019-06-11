package org.constellation.storage

import cats.data.EitherT
import cats.effect.{LiftIO, Sync}
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.DAO
import org.constellation.consensus.{Snapshot, SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.{CheckpointBlock, ConcurrentTipService}
import org.constellation.primitives.Schema.{CheckpointCache, NodeState}
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.util.Metrics

import scala.util.Try

class SnapshotService[F[_]: Sync : LiftIO](
  concurrentTipService: ConcurrentTipService,
  addressService: AddressService[F],
  checkpointService: CheckpointService[F],
  dao: DAO
) {
  import constellation._

  implicit val shadowDao: DAO = dao

  val acceptedCBSinceSnapshot: Ref[F, Seq[String]] = Ref.unsafe(Seq())
  val syncBuffer: Ref[F, Seq[CheckpointCache]] = Ref.unsafe(Seq())
  val snapshot: Ref[F, Snapshot] = Ref.unsafe(Snapshot.snapshotZero)

  val totalNumCBsInSnapshots: Ref[F, Long] = Ref.unsafe(0L)
  val lastSnapshotHeight: Ref[F, Int] = Ref.unsafe(0)

  def getSnapshotInfo(): F[SnapshotInfo] = for {
    s <- snapshot.get
    accepted <- acceptedCBSinceSnapshot.get
    lastHeight <- lastSnapshotHeight.get
    hashes = dao.snapshotHashes
    addressCacheData <- addressService.toMap()
    tips = concurrentTipService.toMap
    snapshotCache <- s.checkpointBlocks
      .toList
      .map(checkpointService.fullData)
      .sequence
      .map(_.flatten)
  } yield SnapshotInfo(
    s,
    accepted,
    lastSnapshotHeight = lastHeight,
    snapshotHashes = hashes,
    addressCacheData = addressCacheData,
    tips = tips,
    snapshotCache = snapshotCache
  )

  def setSnapshot(snapshotInfo: SnapshotInfo): F[Unit] = {
    for {
      _ <- snapshot.set(snapshotInfo.snapshot)
      _ <- lastSnapshotHeight.set(snapshotInfo.lastSnapshotHeight)
      _ <- Sync[F].delay(concurrentTipService.set(snapshotInfo.tips))
      _ <- acceptedCBSinceSnapshot.set(snapshotInfo.acceptedCBSinceSnapshot)
      _ <- snapshotInfo.addressCacheData
              .map { case (k, v) => addressService.put(k, v) }
              .toList
              .sequence
      _ <- (snapshotInfo.snapshotCache ++ snapshotInfo.acceptedCBSinceSnapshotCache)
              .toList
              .map { h =>
                LiftIO[F].liftIO(h.checkpointBlock.get.storeSOE()) *>
                  checkpointService.memPool.put(h.checkpointBlock.get.baseHash, h) *>
                  dao.metrics.incrementMetricAsync(Metrics.checkpointAccepted) *>
                  h.checkpointBlock.get
                    .transactions.toList
                    .map(_ => dao.metrics.incrementMetricAsync("transactionAccepted")).sequence
              }.sequence
      _ <- dao.metrics.updateMetricAsync[F](
        "acceptedCBCacheMatchesAcceptedSize",
        (snapshotInfo.acceptedCBSinceSnapshot.size == snapshotInfo.acceptedCBSinceSnapshotCache.size).toString)
    } yield ()
  }

  def syncBufferAccept(cb: CheckpointCache)(implicit dao: DAO): F[Unit] = for {
    _ <- syncBuffer.update(_ ++ Seq(cb))
    buffer <- syncBuffer.get
    _ <- dao.metrics.updateMetricAsync[F]("syncBufferSize", buffer.size.toString)
  } yield ()

  def attemptSnapshot(): EitherT[F, SnapshotError, Unit] = for {
    _ <- validateMaxAcceptedCBHashesInMemory()
    _ <- validateNodeState(NodeState.Ready)
    _ <- validateAcceptedCBsSinceSnapshot()

    nextHeightInterval <- EitherT.liftF(getNextHeightInterval())
    _ <- validateSnapshotHeightIntervalCondition(nextHeightInterval)
    blocksWithinHeightInterval <- EitherT.liftF(getBlocksWithinHeightInterval(nextHeightInterval))
    _ <- validateBlocksWithinHeightInterval(blocksWithinHeightInterval)
    allBlocks = blocksWithinHeightInterval.map(_.get)

    hashesForNextSnapshot = allBlocks.flatMap(_.checkpointBlock.map(_.baseHash)).sorted
    nextSnapshot <- EitherT.liftF(getNextSnapshot(hashesForNextSnapshot))

    _ <- EitherT.liftF(applySnapshot())

    _ <- EitherT.liftF(lastSnapshotHeight.set(nextHeightInterval.toInt))
    _ <- EitherT.liftF(acceptedCBSinceSnapshot.update(_.filterNot(hashesForNextSnapshot.contains)))
    _ <- EitherT.liftF(updateMetricsAfterSnapshot())

    _ <- EitherT.liftF(snapshot.set(nextSnapshot))

  } yield ()

  def updateAcceptedCBSinceSnapshot(cb: CheckpointBlock): F[Unit] = {
    acceptedCBSinceSnapshot.get.flatMap { accepted =>
      if (accepted.contains(cb.baseHash)) {
        dao.metrics.incrementMetricAsync("checkpointAcceptedButAlreadyInAcceptedCBSinceSnapshot")
      } else {
        acceptedCBSinceSnapshot.update(_ :+ cb.baseHash).flatTap { _ =>
          dao.metrics.updateMetricAsync("acceptedCBSinceSnapshot", accepted.size + 1)
        }
      }
    }
  }

  private def validateMaxAcceptedCBHashesInMemory(): EitherT[F, SnapshotError, Unit] = EitherT {
    acceptedCBSinceSnapshot
      .get
      .map { accepted =>
        if (accepted.size > dao.processingConfig.maxAcceptedCBHashesInMemory)
          Left(MaxCBHashesInMemory)
        else
          Right(())
      }
      .flatMap { e =>
        val tap = if (e.isLeft) {
          acceptedCBSinceSnapshot.update(accepted => accepted.slice(0, 100)) *>
            dao.metrics.incrementMetricAsync[F]("memoryExceeded_acceptedCBSinceSnapshot") *>
            acceptedCBSinceSnapshot.get.flatMap { accepted =>
              dao.metrics.updateMetricAsync[F]("acceptedCBSinceSnapshot", accepted.size.toString)
            }
        } else Sync[F].unit

        tap.map(_ => e)
      }
  }

  private def validateNodeState(requiredState: NodeState): EitherT[F, SnapshotError, Unit] = EitherT {
    Sync[F].delay {
      if (dao.nodeState == requiredState) {
        Right(())
      } else {
        Left(NodeNotReadyForSnapshots)
      }
    }
  }

  private def validateAcceptedCBsSinceSnapshot(): EitherT[F, SnapshotError, Unit] = EitherT {
    acceptedCBSinceSnapshot.get.map { accepted => accepted.size match {
      case 0 => Left(NoAcceptedCBsSinceSnapshot)
      case _ => Right(())
    }}
  }

  private def validateSnapshotHeightIntervalCondition(nextHeightInterval: Long): EitherT[F, SnapshotError, Unit] = EitherT {
    val minTipHeight = Try { concurrentTipService.getMinTipHeight() }.getOrElse(0L)
    val snapshotHeightDelayInterval = dao.processingConfig.snapshotHeightDelayInterval

    dao.metrics.updateMetricAsync[F]("minTipHeight", minTipHeight.toString) *>
      Sync[F].pure {
        if (minTipHeight > (nextHeightInterval + snapshotHeightDelayInterval)) {
          Right(())
        } else {
          Left(HeightIntervalConditionNotMet)
        }
      }
      .flatMap { e =>
        val tap = if (e.isLeft) {
          dao.metrics.incrementMetricAsync[F]("snapshotHeightIntervalConditionNotMet")
        } else {
          dao.metrics.incrementMetricAsync[F]("snapshotHeightIntervalConditionMet")
        }

        tap.map(_ => e)
      }
  }

  private def getNextHeightInterval(): F[Long] =
    lastSnapshotHeight.get
      .map(_ + dao.processingConfig.snapshotHeightInterval)

  private def getBlocksWithinHeightInterval(nextHeightInterval: Long): F[List[Option[CheckpointCache]]] = {
    for {
      height <- lastSnapshotHeight.get

      maybeDatas <- acceptedCBSinceSnapshot.get.flatMap(_.toList.map(checkpointService.fullData).sequence)

      blocks = maybeDatas.filter {
        _.exists(_.height.exists { h =>
          h.min > height && h.min <= nextHeightInterval
        })
      }
    } yield blocks
  }

  private def validateBlocksWithinHeightInterval(
    blocks: List[Option[CheckpointCache]]
  ): EitherT[F, SnapshotError, Unit] = EitherT {
    Sync[F].pure {
      if (blocks.isEmpty) {
        Left(NoBlocksWithinHeightInterval)
      } else {
        Right(())
      }
    }.flatMap { e =>
      val tap = if (e.isLeft) {
        dao.metrics.incrementMetricAsync("snapshotNoBlocksWithinHeightInterval")
      } else Sync[F].unit

      tap.map(_ => e)
    }
  }

  private def getNextSnapshot(hashesForNextSnapshot: Seq[String]): F[Snapshot] = {
    snapshot.get
      .map(_.hash)
      .map(hash => Snapshot(hash, hashesForNextSnapshot))
  }

  private def applySnapshot(): F[Unit] = {
    snapshot.get.flatMap { currentSnapshot =>
      if (currentSnapshot == Snapshot.snapshotZero) {
        Sync[F].unit
      } else {
        dao.metrics.incrementMetricAsync(Metrics.snapshotCount) *>
          writeSnapshotToDisk(currentSnapshot) *>
          applyAfterSnapshot(currentSnapshot)
      }
    }
  }

  private def writeSnapshotToDisk(currentSnapshot: Snapshot): F[Unit] = {
    currentSnapshot.checkpointBlocks
      .toList
      .map(checkpointService.fullData)
      .sequence
      .map {
        case maybeBlocks if maybeBlocks.exists(_.exists(_.checkpointBlock.isEmpty)) =>
          // TODO : This should never happen, if it does we need to reset the node state and redownload
          dao.metrics.incrementMetricAsync("snapshotWriteToDiskMissingData")

        case maybeBlocks =>
          val flatten = maybeBlocks.flatten.sortBy(_.checkpointBlock.map(_.baseHash))
          Sync[F].delay {
            tryWithMetric(
              Snapshot.writeSnapshot(StoredSnapshot(currentSnapshot, flatten)),
              "snapshotWriteToDisk"
            )
          }
      }
  }

  private def applyAfterSnapshot(currentSnapshot: Snapshot): F[Unit] = {
    currentSnapshot
      .checkpointBlocks
      .toList
      .map(checkpointService.fullData)
      .sequence
      .map(_.map(_.flatMap(_.checkpointBlock)).sequence)
      .map(_.map(_.flatMap(_.transactions.toList.flatMap(t => List(t.src, t.dst))).toSet))
      .flatMap { addresses =>
        addressService.lockForSnapshot(
          addresses.get,
          Sync[F].delay(Snapshot.acceptSnapshot(currentSnapshot)) *>
            totalNumCBsInSnapshots.update(_ + currentSnapshot.checkpointBlocks.size) *>
            totalNumCBsInSnapshots.get.flatTap { total =>
              dao.metrics.updateMetricAsync("totalNumCBsInShapshots", total.toString)
            } *>
            checkpointService.applySnapshot(currentSnapshot.checkpointBlocks.toList) *>
            dao.metrics.updateMetricAsync(Metrics.lastSnapshotHash, currentSnapshot.hash)
          )
      }
  }

  private def updateMetricsAfterSnapshot(): F[Unit] = {
    for {
      accepted <- acceptedCBSinceSnapshot.get
      height <- lastSnapshotHeight.get
      nextHeight = height + dao.processingConfig.snapshotHeightInterval

      _ <- dao.metrics.updateMetricAsync("acceptedCBSinceSnapshot", accepted.size)
      _ <- dao.metrics.updateMetricAsync("lastSnapshotHeight", height)
      _ <- dao.metrics.updateMetricAsync("nextSnapshotHeight", nextHeight)
    } yield ()
  }
}

object SnapshotService {
  def apply[F[_]: Sync : LiftIO](
    concurrentTipService: ConcurrentTipService,
    addressService: AddressService[F],
    checkpointService: CheckpointService[F],
    dao: DAO
  ) =
    new SnapshotService[F](concurrentTipService, addressService, checkpointService, dao)
}

sealed trait SnapshotError

object MaxCBHashesInMemory extends SnapshotError
object NodeNotReadyForSnapshots extends SnapshotError
object NoAcceptedCBsSinceSnapshot extends SnapshotError
object HeightIntervalConditionNotMet extends SnapshotError
object NoBlocksWithinHeightInterval extends SnapshotError