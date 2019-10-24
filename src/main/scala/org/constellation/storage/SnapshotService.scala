package org.constellation.storage

import java.nio.file.Path

import cats.data.EitherT
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.checkpoint.CheckpointService
import org.constellation.consensus.{ConsensusManager, Snapshot, SnapshotInfo, StoredSnapshot}
import org.constellation.domain.transaction.TransactionService
import org.constellation.p2p.{Cluster, DataResolver}
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.primitives._
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.util.Metrics
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

class SnapshotService[F[_]: Concurrent](
  concurrentTipService: ConcurrentTipService[F],
  addressService: AddressService[F],
  checkpointService: CheckpointService[F],
  messageService: MessageService[F],
  transactionService: TransactionService[F],
  rateLimiting: RateLimiting[F],
  consensusManager: ConsensusManager[F],
  dao: DAO
)(implicit C: ContextShift[F])
    extends StrictLogging {

  implicit val shadowDao: DAO = dao

  val acceptedCBSinceSnapshot: SingleRef[F, Seq[String]] = SingleRef(Seq())
  val syncBuffer: SingleRef[F, Seq[CheckpointCache]] = SingleRef(Seq())
  val snapshot: SingleRef[F, Snapshot] = SingleRef(Snapshot.snapshotZero)

  val totalNumCBsInSnapshots: SingleRef[F, Long] = SingleRef(0L)
  val lastSnapshotHeight: SingleRef[F, Int] = SingleRef(0)
  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")
  val snapshotHeightDelayInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightDelayInterval")

  def exists(hash: String): F[Boolean] =
    for {
      last <- snapshot.get
      hashes = Snapshot.snapshotHashes()
    } yield last.hash == hash || hashes.contains(hash)

  def getLastSnapshotHeight: F[Int] = lastSnapshotHeight.get

  def getSnapshotInfo: F[SnapshotInfo] =
    for {
      s <- snapshot.get
      accepted <- acceptedCBSinceSnapshot.get
      lastHeight <- lastSnapshotHeight.get
      hashes = dao.snapshotHashes
      addressCacheData <- addressService.toMap()
      tips <- concurrentTipService.toMap
      snapshotCache <- s.checkpointBlocks.toList
        .map(checkpointService.fullData)
        .sequence
        .map(_.flatten)
    } yield
      SnapshotInfo(
        s,
        accepted,
        lastSnapshotHeight = lastHeight,
        snapshotHashes = hashes,
        addressCacheData = addressCacheData,
        tips = tips,
        snapshotCache = snapshotCache
      )

  def retainOldData(): F[Unit] =
    for {
      snap <- snapshot.get
      accepted <- acceptedCBSinceSnapshot.get
      cbs = (snap.checkpointBlocks ++ accepted).toList
      fetched <- getCheckpointBlocksFromSnapshot(cbs)
      _ <- fetched.traverse(_.transactionsMerkleRoot.traverse(transactionService.applySnapshot))
      _ <- checkpointService.applySnapshot(cbs)
    } yield ()

  def setSnapshot(snapshotInfo: SnapshotInfo): F[Unit] =
    for {
      _ <- retainOldData()
      _ <- snapshot.set(snapshotInfo.snapshot)
      _ <- lastSnapshotHeight.set(snapshotInfo.lastSnapshotHeight)
      _ <- concurrentTipService.set(snapshotInfo.tips)
      _ <- acceptedCBSinceSnapshot.set(snapshotInfo.acceptedCBSinceSnapshot)
      _ <- snapshotInfo.addressCacheData.map { case (k, v) => addressService.put(k, v) }.toList.sequence
      _ <- (snapshotInfo.snapshotCache ++ snapshotInfo.acceptedCBSinceSnapshotCache).toList.map { h =>
        LiftIO[F].liftIO(h.checkpointBlock.get.storeSOE()) >>
          checkpointService.put(h) >>
          dao.metrics.incrementMetricAsync(Metrics.checkpointAccepted) >>
          h.checkpointBlock.get.transactions.toList
            .map(
              tx =>
                transactionService
                  .accept(TransactionCacheData(tx))
                  .flatTap(_ => dao.metrics.incrementMetricAsync("transactionAccepted"))
            )
            .sequence
      }.sequence
      _ <- dao.metrics.updateMetricAsync[F](
        "acceptedCBCacheMatchesAcceptedSize",
        (snapshotInfo.acceptedCBSinceSnapshot.size == snapshotInfo.acceptedCBSinceSnapshotCache.size).toString
      )
      _ <- updateMetricsAfterSnapshot()
    } yield ()

  def syncBufferAccept(cb: CheckpointCache): F[Unit] =
    for {
      _ <- syncBuffer.update(_ ++ Seq(cb))
      buffer <- syncBuffer.get
      _ <- dao.metrics.updateMetricAsync[F]("syncBufferSize", buffer.size.toString)
    } yield ()

  def attemptSnapshot()(implicit cluster: Cluster[F]): EitherT[F, SnapshotError, SnapshotCreated] =
    for {
      _ <- validateMaxAcceptedCBHashesInMemory()
      _ <- validateAcceptedCBsSinceSnapshot()

      nextHeightInterval <- EitherT.liftF(getNextHeightInterval)
      minActiveTipHeight <- EitherT.liftF(LiftIO[F].liftIO(dao.getActiveMinHeight))
      minTipHeight <- EitherT.liftF(concurrentTipService.getMinTipHeight(minActiveTipHeight))
      _ <- validateSnapshotHeightIntervalCondition(nextHeightInterval, minTipHeight)
      blocksWithinHeightInterval <- EitherT.liftF(getBlocksWithinHeightInterval(nextHeightInterval))
      _ <- validateBlocksWithinHeightInterval(blocksWithinHeightInterval)
      allBlocks = blocksWithinHeightInterval.map(_.get)

      hashesForNextSnapshot = allBlocks.flatMap(_.checkpointBlock.map(_.baseHash)).sorted
      nextSnapshot <- EitherT.liftF(getNextSnapshot(hashesForNextSnapshot))

      _ <- EitherT.liftF(
        Sync[F].delay(
          logger.debug(
            s"conclude snapshot: ${nextSnapshot.lastSnapshot} with height ${nextHeightInterval - snapshotHeightDelayInterval}"
          )
        )
      )
      _ <- applySnapshot()
      _ <- EitherT.liftF(lastSnapshotHeight.set(nextHeightInterval.toInt))
      _ <- EitherT.liftF(acceptedCBSinceSnapshot.update(_.filterNot(hashesForNextSnapshot.contains)))
      _ <- EitherT.liftF(calculateAcceptedTransactionsSinceSnapshot())
      _ <- EitherT.liftF(updateMetricsAfterSnapshot())

      _ <- EitherT.liftF(snapshot.set(nextSnapshot))

      _ <- EitherT.liftF(removeLeavingPeers())

      created <- EitherT.liftF(
        Sync[F].pure(SnapshotCreated(nextSnapshot.lastSnapshot, nextHeightInterval - snapshotHeightInterval))
      )
    } yield created

  def updateAcceptedCBSinceSnapshot(cb: CheckpointBlock): F[Unit] =
    acceptedCBSinceSnapshot.get.flatMap { accepted =>
      if (accepted.contains(cb.baseHash)) {
        dao.metrics.incrementMetricAsync("checkpointAcceptedButAlreadyInAcceptedCBSinceSnapshot")
      } else {
        acceptedCBSinceSnapshot.update(_ :+ cb.baseHash).flatTap { _ =>
          dao.metrics.updateMetricAsync("acceptedCBSinceSnapshot", accepted.size + 1)
        }
      }
    }

  def calculateAcceptedTransactionsSinceSnapshot(): F[Unit] =
    for {
      cbHashes <- acceptedCBSinceSnapshot.get.map(_.toList)
      _ <- rateLimiting.reset(cbHashes)(checkpointService)
    } yield ()

  private def validateMaxAcceptedCBHashesInMemory(): EitherT[F, SnapshotError, Unit] = EitherT {
    acceptedCBSinceSnapshot.get.map { accepted =>
      if (accepted.size > dao.processingConfig.maxAcceptedCBHashesInMemory)
        Left(MaxCBHashesInMemory)
      else
        Right(())
    }.flatMap { e =>
      val tap = if (e.isLeft) {
        acceptedCBSinceSnapshot.update(accepted => accepted.slice(0, 100)) >>
          dao.metrics.incrementMetricAsync[F]("memoryExceeded_acceptedCBSinceSnapshot") >>
          acceptedCBSinceSnapshot.get.flatMap { accepted =>
            dao.metrics.updateMetricAsync[F]("acceptedCBSinceSnapshot", accepted.size.toString)
          }
      } else Sync[F].unit

      tap.map(_ => e)
    }
  }

  private def validateAcceptedCBsSinceSnapshot(): EitherT[F, SnapshotError, Unit] = EitherT {
    acceptedCBSinceSnapshot.get.map { accepted =>
      accepted.size match {
        case 0 => Left(NoAcceptedCBsSinceSnapshot)
        case _ => Right(())
      }
    }
  }

  private def validateSnapshotHeightIntervalCondition(
    nextHeightInterval: Long,
    minTipHeight: Long
  ): EitherT[F, SnapshotError, Unit] =
    EitherT {

      dao.metrics.updateMetricAsync[F]("minTipHeight", minTipHeight.toString) >>
        Sync[F].pure {
          if (minTipHeight > (nextHeightInterval + snapshotHeightDelayInterval)) {
            logger.debug(
              s"height interval met minTipHeight: $minTipHeight nextHeightInterval: $nextHeightInterval and ${nextHeightInterval + snapshotHeightDelayInterval}"
            )
            Right(())
          } else {
            logger.debug(
              s"height interval not met minTipHeight: $minTipHeight nextHeightInterval: $nextHeightInterval and ${nextHeightInterval + snapshotHeightDelayInterval}"
            )
            Left(HeightIntervalConditionNotMet)
          }
        }.flatMap { e =>
          val tap = if (e.isLeft) {
            dao.metrics.incrementMetricAsync[F]("snapshotHeightIntervalConditionNotMet")
          } else {
            dao.metrics.incrementMetricAsync[F]("snapshotHeightIntervalConditionMet")
          }

          tap.map(_ => e)
        }
    }

  def getNextHeightInterval: F[Long] =
    lastSnapshotHeight.get
      .map(_ + snapshotHeightInterval)

  private def getBlocksWithinHeightInterval(nextHeightInterval: Long): F[List[Option[CheckpointCache]]] =
    for {
      height <- lastSnapshotHeight.get

      maybeDatas <- acceptedCBSinceSnapshot.get.flatMap(_.toList.traverse(checkpointService.fullData))

      blocks = maybeDatas.filter {
        _.exists(_.height.exists { h =>
          h.min > height && h.min <= nextHeightInterval
        })
      }
      _ <- Sync[F].delay(
        logger.debug(s"blocks for snapshot between lastSnapshotHeight: $height nextHeightInterval: $nextHeightInterval")
      )
    } yield blocks

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

  private def getNextSnapshot(hashesForNextSnapshot: Seq[String]): F[Snapshot] =
    snapshot.get
      .map(_.hash)
      .map(hash => Snapshot(hash, hashesForNextSnapshot))

  private[storage] def applySnapshot()(implicit C: ContextShift[F]): EitherT[F, SnapshotError, Unit] = {
    val write: Snapshot => EitherT[F, SnapshotError, Unit] = (currentSnapshot: Snapshot) =>
      writeSnapshotToDisk(currentSnapshot)
        .flatMap(
          _ => applyAfterSnapshot(currentSnapshot)
        )

    snapshot.get.attemptT
      .leftMap(SnapshotUnexpectedError)
      .flatMap { currentSnapshot =>
        if (currentSnapshot == Snapshot.snapshotZero) EitherT.rightT[F, SnapshotError](())
        else write(currentSnapshot)
      }
  }

  def addSnapshotToDisk(snapshot: StoredSnapshot): EitherT[F, Throwable, Path] =
    Snapshot.writeSnapshot[F](snapshot)

  private def writeSnapshotToDisk(
    currentSnapshot: Snapshot
  )(implicit C: ContextShift[F]): EitherT[F, SnapshotError, Unit] =
    currentSnapshot.checkpointBlocks.toList
      .traverse(h => checkpointService.fullData(h).map(d => (h, d)))
      .attemptT
      .leftMap(SnapshotUnexpectedError)
      .flatMap {
        case maybeBlocks
            if maybeBlocks.exists(
              maybeCache => maybeCache._2.isEmpty || maybeCache._2.exists(_.checkpointBlock.isEmpty)
            ) =>
          EitherT {
            Sync[F].delay {
              val maybeEmpty =
                maybeBlocks.find(maybeCache => maybeCache._2.isEmpty || maybeCache._2.exists(_.checkpointBlock.isEmpty))
              logger.error(s"Snapshot data is missing for block: ${maybeEmpty}")
            }.flatTap(_ => dao.metrics.incrementMetricAsync("snapshotInvalidData"))
              .map(_ => Left(SnapshotIllegalState))
          }

        case maybeBlocks =>
          val flatten = maybeBlocks.flatMap(_._2).sortBy(_.checkpointBlock.map(_.baseHash))
          Snapshot
            .writeSnapshot(StoredSnapshot(currentSnapshot, flatten))
            .biSemiflatMap(
              t =>
                dao.metrics
                  .incrementMetricAsync(Metrics.snapshotWriteToDisk + Metrics.failure)
                  .map(_ => SnapshotIOError(t)),
              p =>
                Sync[F]
                  .delay(logger.debug(s"Snapshot written at path: ${p.toAbsolutePath.toString}"))
                  .flatMap(_ => dao.metrics.incrementMetricAsync(Metrics.snapshotWriteToDisk + Metrics.success))
            )
      }

  private def applyAfterSnapshot(currentSnapshot: Snapshot): EitherT[F, SnapshotError, Unit] = {
    val applyAfter = for {
      _ <- acceptSnapshot(currentSnapshot)

      _ <- totalNumCBsInSnapshots.update(_ + currentSnapshot.checkpointBlocks.size)
      _ <- totalNumCBsInSnapshots.get.flatTap { total =>
        dao.metrics.updateMetricAsync("totalNumCBsInShapshots", total.toString)
      }

      _ <- checkpointService.applySnapshot(currentSnapshot.checkpointBlocks.toList)
      _ <- dao.metrics.updateMetricAsync(Metrics.lastSnapshotHash, currentSnapshot.hash)
      _ <- dao.metrics.incrementMetricAsync(Metrics.snapshotCount)
    } yield ()

    applyAfter.attemptT
      .leftMap(SnapshotUnexpectedError)
  }

  private def updateMetricsAfterSnapshot(): F[Unit] =
    for {
      accepted <- acceptedCBSinceSnapshot.get
      height <- lastSnapshotHeight.get
      nextHeight = height + snapshotHeightInterval

      _ <- dao.metrics.updateMetricAsync("acceptedCBSinceSnapshot", accepted.size)
      _ <- dao.metrics.updateMetricAsync("lastSnapshotHeight", height)
      _ <- dao.metrics.updateMetricAsync("nextSnapshotHeight", nextHeight)
    } yield ()

  private def acceptSnapshot(s: Snapshot): F[Unit] =
    for {
      cbs <- getCheckpointBlocksFromSnapshot(s.checkpointBlocks.toList)
      _ <- cbs.traverse(applySnapshotMessages(s, _))
      _ <- applySnapshotTransactions(s, cbs)
    } yield ()

  private def getCheckpointBlocksFromSnapshot(blocks: List[String]): F[List[CheckpointBlockMetadata]] =
    for {
      cbData <- blocks.map(checkpointService.lookup).sequence

      _ <- if (cbData.exists(_.isEmpty)) {
        dao.metrics.incrementMetricAsync("snapshotCBAcceptQueryFailed")
      } else Sync[F].unit

      cbs = cbData.flatten.map(_.checkpointBlock)
    } yield cbs

  private def applySnapshotMessages(s: Snapshot, cb: CheckpointBlockMetadata): F[Unit] = {

    def updateMessage(msgHash: String, channelMessage: ChannelMessage) =
      messageService.memPool.update(
        msgHash,
        _.copy(snapshotHash = Some(s.hash), blockHash = Some(cb.baseHash)),
        ChannelMessageMetadata(
          channelMessage,
          Some(cb.baseHash),
          Some(s.hash)
        )
      )

    cb.messagesMerkleRoot.traverse {
      messageService
        .findHashesByMerkleRoot(_)
        .flatMap(_.get.toList.traverse { msgHash =>
          dao.metrics.incrementMetricAsync("messageSnapshotHashUpdated") >>
            LiftIO[F]
              .liftIO(
                DataResolver
                  .resolveMessageDefaults(msgHash)(IO.contextShift(ConstellationExecutionContext.bounded))
                  .map(_.channelMessage)
              )
              .flatMap(updateMessage(msgHash, _))
        })
    }.void
  }

  private def applySnapshotTransactions(s: Snapshot, cbs: List[CheckpointBlockMetadata]): F[Unit] =
    for {
      txs <- cbs
        .traverse(_.transactionsMerkleRoot.traverse(checkpointService.fetchBatchTransactions).map(_.getOrElse(List())))
        .map(_.flatten)

      _ <- txs
        .filterNot(_.isDummy)
        .traverse(t => addressService.lockForSnapshot(Set(t.src, t.dst), addressService.transferSnapshot(t).void))

      _ <- cbs.traverse(
        _.transactionsMerkleRoot.traverse(transactionService.applySnapshot(txs.map(TransactionCacheData(_)), _))
      )
    } yield ()

  private def removeLeavingPeers(): F[Unit] =
    LiftIO[F].liftIO(dao.leavingPeers.flatMap(_.values.toList.traverse(dao.cluster.removeDeadPeer))).void
}

object SnapshotService {

  def apply[F[_]: Concurrent](
    concurrentTipService: ConcurrentTipService[F],
    addressService: AddressService[F],
    checkpointService: CheckpointService[F],
    messageService: MessageService[F],
    transactionService: TransactionService[F],
    rateLimiting: RateLimiting[F],
    consensusManager: ConsensusManager[F],
    dao: DAO
  )(implicit C: ContextShift[F]) =
    new SnapshotService[F](
      concurrentTipService,
      addressService,
      checkpointService,
      messageService,
      transactionService,
      rateLimiting,
      consensusManager,
      dao
    )
}

sealed trait SnapshotError

object MaxCBHashesInMemory extends SnapshotError
object NodeNotReadyForSnapshots extends SnapshotError
object NoAcceptedCBsSinceSnapshot extends SnapshotError
object HeightIntervalConditionNotMet extends SnapshotError
object NoBlocksWithinHeightInterval extends SnapshotError
object SnapshotIllegalState extends SnapshotError
case class SnapshotIOError(cause: Throwable) extends SnapshotError
case class SnapshotUnexpectedError(cause: Throwable) extends SnapshotError

case class SnapshotCreated(hash: String, height: Long)
