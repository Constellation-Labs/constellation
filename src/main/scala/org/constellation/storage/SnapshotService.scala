package org.constellation.storage

import java.io.FileOutputStream
import java.nio.file.Path

import better.files.File
import cats.Parallel
import cats.data.EitherT
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, LiftIO, Resource, Sync, _}
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.{CheckpointAcceptanceService, CheckpointService}
import org.constellation.consensus._
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.snapshot.SnapshotStorage
import org.constellation.domain.transaction.TransactionService
import org.constellation.p2p.{Cluster, DataResolver}
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.primitives._
import org.constellation.rewards.RewardsManager
import org.constellation.schema.Id
import org.constellation.storage.external.CloudStorage
import org.constellation.trust.TrustManager
import org.constellation.util.Metrics
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

class SnapshotService[F[_]: Concurrent](
  concurrentTipService: ConcurrentTipService[F],
  cloudStorage: CloudStorage[F],
  addressService: AddressService[F],
  checkpointService: CheckpointService[F],
  messageService: MessageService[F],
  transactionService: TransactionService[F],
  observationService: ObservationService[F],
  rateLimiting: RateLimiting[F],
  consensusManager: ConsensusManager[F],
  trustManager: TrustManager[F],
  soeService: SOEService[F],
  rewardsManager: RewardsManager[F],
  snapshotStorage: SnapshotStorage[F],
  dao: DAO
)(implicit C: ContextShift[F], P: Parallel[F]) {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  implicit val shadowDao: DAO = dao

  val acceptedCBSinceSnapshot: Ref[F, Seq[String]] = Ref.unsafe(Seq.empty)
  val syncBuffer: Ref[F, Map[String, FinishedCheckpoint]] = Ref.unsafe(Map.empty)
  val storedSnapshot: Ref[F, StoredSnapshot] = Ref.unsafe(StoredSnapshot(Snapshot.snapshotZero, Seq.empty))

  val totalNumCBsInSnapshots: Ref[F, Long] = Ref.unsafe(0L)
  val lastSnapshotHeight: Ref[F, Int] = Ref.unsafe(0)
  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")
  val snapshotHeightDelayInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightDelayInterval")

  val recentSnapshotInfo = new StorageService[F, SnapshotInfoSer]("recent_snapshot_info".some, 2.some)

  def exists(hash: String): F[Boolean] =
    for {
      last <- storedSnapshot.get
      hashes <- snapshotStorage.getSnapshotHashes
    } yield last.snapshot.hash == hash || hashes.contains(hash)

  def isStored(hash: String): F[Boolean] =
    snapshotStorage.getSnapshotHashes.map(_.contains(hash))

  def getLastSnapshotHeight: F[Int] = lastSnapshotHeight.get

  def getAcceptedCBSinceSnapshot: F[Seq[String]] =
    for {
      hashes <- acceptedCBSinceSnapshot.get
    } yield hashes

  def attemptSnapshot()(implicit cluster: Cluster[F]): EitherT[F, SnapshotError, RecentSnapshot] =
    for {
      _ <- validateMaxAcceptedCBHashesInMemory()
      _ <- validateAcceptedCBsSinceSnapshot()

      nextHeightInterval <- EitherT.liftF(getNextHeightInterval)
      minActiveTipHeight <- EitherT.liftF(LiftIO[F].liftIO(dao.getActiveMinHeight))
      minTipHeight <- EitherT.liftF(concurrentTipService.getMinTipHeight(minActiveTipHeight))
      _ <- validateSnapshotHeightIntervalCondition(nextHeightInterval, minTipHeight)
      blocksWithinHeightInterval <- EitherT.liftF(getBlocksWithinHeightInterval(nextHeightInterval))
      _ <- validateBlocksWithinHeightInterval(blocksWithinHeightInterval)
      allBlocks = blocksWithinHeightInterval.map(_.get).sortBy(_.checkpointBlock.baseHash)

      hashesForNextSnapshot = allBlocks.map(_.checkpointBlock.baseHash)
      publicReputation <- EitherT.liftF(trustManager.getPredictedReputation)
      nextSnapshot <- EitherT.liftF(getNextSnapshot(hashesForNextSnapshot, publicReputation))

      _ <- EitherT.liftF(
        logger.debug(
          s"Blocks for the next snapshot hash=${nextSnapshot.hash} lastSnapshot=${nextSnapshot.lastSnapshot} at height: ${nextHeightInterval} - ${hashesForNextSnapshot}"
        )
      )

      _ <- EitherT.liftF(
        logger.debug(
          s"conclude snapshot hash=${nextSnapshot.hash} lastSnapshot=${nextSnapshot.lastSnapshot} with height ${nextHeightInterval}"
        )
      )
      _ <- applySnapshot()
      _ <- EitherT.liftF(lastSnapshotHeight.set(nextHeightInterval.toInt))
      _ <- EitherT.liftF(acceptedCBSinceSnapshot.update(_.filterNot(hashesForNextSnapshot.contains)))
      _ <- EitherT.liftF(calculateAcceptedTransactionsSinceSnapshot())
      _ <- EitherT.liftF(updateMetricsAfterSnapshot())

      _ <- EitherT.liftF(storedSnapshot.set(StoredSnapshot(nextSnapshot, allBlocks.toSeq)))

      _ <- EitherT.liftF(removeLeavingPeers())

      _ <- EitherT.liftF(rewardsManager.attemptReward(nextSnapshot, nextHeightInterval))

      created = RecentSnapshot(
        nextSnapshot.hash,
        nextHeightInterval,
        publicReputation
      )
    } yield created

  def writeSnapshotFile(path: String, part: Array[Byte]): F[Unit] =
    Resource
      .fromAutoCloseable(Sync[F].delay(new FileOutputStream(path)))
      .use(
        stream =>
          Sync[F].delay {
            stream.write(part)
          }.flatTap { _ =>
            logger.debug(s"SnapshotInfo part written for path: $path")
          }
      )

  def writeSnapshotPart(path: String, part: Array[Byte]): F[Unit] =
    for {
      _ <- writeSnapshotFile(path, part)
      _ <- if (ConfigUtil.isEnabledCloudStorage) cloudStorage.upload(Seq(File(path))).void else Sync[F].unit
    } yield ()

  def snapshotInfoWriterProc(plan: Seq[(String, Array[Byte])], basePath: String): F[List[Unit]] =
    plan.toList.parTraverse {
      case (path, part) => writeSnapshotPart(File(basePath, path).pathAsString, part)
    }

  def writeSnapshotInfoParts(info: SnapshotInfo, basePath: String): F[List[Unit]] =
    info.toSnapshotInfoSer().write[F[List[Unit]]](snapshotInfoWriterProc)(basePath = basePath)

  def snapshotInfoFileHeightPrefix(height: Long): String = s"${height}_"

  def writeSnapshotInfoToDisk(
    overWritePath: String = dao.snapshotInfoPath.pathAsString
  ): EitherT[F, SnapshotInfoIOError, Unit] =
    EitherT.liftF {
      getSnapshotInfoWithFullData.flatMap { info =>
      val heightPrefix = snapshotInfoFileHeightPrefix(info.lastSnapshotHeight)
      val hash = info.snapshot.snapshot.hash

        val path = File(overWritePath.concat(s"/$heightPrefix$hash/"))
          .createDirectoryIfNotExists()

        if (info.snapshot.snapshot == Snapshot.snapshotZero) Sync[F].unit
        else writeSnapshotInfoParts(info, path.pathAsString).map(_ => ())
      }
    }.leftMap(SnapshotInfoIOError)


  def getSnapshotInfo(): F[SnapshotInfo] =
    for {
      s <- storedSnapshot.get
      accepted <- acceptedCBSinceSnapshot.get
      lastHeight <- lastSnapshotHeight.get
      hashes <- snapshotStorage.getSnapshotHashes
      addressCacheData <- addressService.toMap
      tips <- concurrentTipService.toMap
      lastAcceptedTransactionRef <- transactionService.transactionChainService.getLastAcceptedTransactionMap()
    } yield
      SnapshotInfo(
        s,
        accepted,
        lastSnapshotHeight = lastHeight,
        snapshotHashes = hashes.toList,
        addressCacheData = addressCacheData,
        tips = tips,
        snapshotCache = s.checkpointCache.toList,
        lastAcceptedTransactionRef = lastAcceptedTransactionRef
      )

  def retainOldData(): F[Unit] =
    for {
      snap <- storedSnapshot.get
      accepted <- acceptedCBSinceSnapshot.get
      cbs = (snap.snapshot.checkpointBlocks ++ accepted).toList
      fetched <- getCheckpointBlocksFromSnapshot(cbs)
      _ <- fetched.traverse(_.transactionsMerkleRoot.traverse(transactionService.applySnapshot))
      _ <- fetched.traverse(_.observationsMerkleRoot.traverse(observationService.applySnapshot))
      soeHashes <- getSOEHashesFrom(cbs)
      _ <- checkpointService.applySnapshot(cbs)
      _ <- soeService.applySnapshot(soeHashes)
      _ <- logger.info(s"Removed soeHashes : $soeHashes")
    } yield ()

  def getLocalAcceptedCBSinceSnapshotCache(snapHashes: Seq[String]): F[List[CheckpointCache]] =
    snapHashes.toList.traverse(str => checkpointService.fullData(str)).map(lstOpts => lstOpts.flatten)

  def getCheckpointAcceptanceService = LiftIO[F].liftIO(dao.checkpointAcceptanceService.awaiting.get)

  def setSnapshot(snapshotInfo: SnapshotInfo): F[Unit] =
    for {
      _ <- retainOldData()
      _ <- storedSnapshot.modify(_ => (snapshotInfo.snapshot, ()))
      _ <- lastSnapshotHeight.modify(_ => (snapshotInfo.lastSnapshotHeight, ()))
      _ <- LiftIO[F].liftIO(dao.checkpointAcceptanceService.awaiting.modify(_ => (snapshotInfo.awaitingCbs, ())))
      _ <- concurrentTipService.set(snapshotInfo.tips)
      _ <- acceptedCBSinceSnapshot.modify(_ => (snapshotInfo.acceptedCBSinceSnapshot, ()))
      _ <- transactionService.transactionChainService.applySnapshotInfo(snapshotInfo)
      _ <- snapshotInfo.addressCacheData.map { case (k, v) => addressService.putUnsafe(k, v) }.toList.sequence
      _ <- (snapshotInfo.snapshotCache ++ snapshotInfo.acceptedCBSinceSnapshotCache).toList.traverse { h =>
        soeService.put(h.checkpointBlock.soeHash, h.checkpointBlock.soe) >>
          checkpointService.put(h) >>
          dao.metrics.incrementMetricAsync(Metrics.checkpointAccepted) >>
          h.checkpointBlock.transactions.toList.traverse { tx =>
            transactionService.applyAfterRedownload(TransactionCacheData(tx), Some(h))
          } >>
          h.checkpointBlock.observations.toList.traverse { obs =>
            observationService.applyAfterRedownload(obs, Some(h))
          }
      }
      _ <- dao.metrics.updateMetricAsync[F](
        "acceptedCBCacheMatchesAcceptedSize",
        (snapshotInfo.acceptedCBSinceSnapshot.size == snapshotInfo.acceptedCBSinceSnapshotCache.size).toString
      )
      _ <- logger.info(
        s"acceptedCBCacheMatchesAcceptedSize size: ${(snapshotInfo.acceptedCBSinceSnapshot.size == snapshotInfo.acceptedCBSinceSnapshotCache.size).toString}"
      )
      _ <- logger.info(
        s"acceptedCBCacheMatchesAcceptedSize diff: ${snapshotInfo.acceptedCBSinceSnapshot.toList.diff(snapshotInfo.acceptedCBSinceSnapshotCache)}"
      )
      _ <- updateMetricsAfterSnapshot()
    } yield ()

  def syncBufferAccept(cb: FinishedCheckpoint): F[Unit] =
    for {
      size <- syncBuffer.modify { curr =>
        val updated = curr + (cb.checkpointCacheData.checkpointBlock.baseHash -> cb)
        (updated, updated.size)
      }
      _ <- dao.metrics.updateMetricAsync[F]("syncBufferSize", size.toString)
    } yield ()

  def syncBufferPull(): F[Map[String, FinishedCheckpoint]] =
    for {
      pulled <- syncBuffer.modify(curr => (Map.empty, curr))
      _ <- dao.metrics.updateMetricAsync[F]("syncBufferSize", pulled.size.toString)
    } yield pulled

  def getSnapshotInfoWithFullData: F[SnapshotInfo] =
    getSnapshotInfo.flatMap { info =>
      LiftIO[F].liftIO(
        info.acceptedCBSinceSnapshot.toList.traverse {
          dao.checkpointService.fullData(_)

        }.map(cbs => info.copy(acceptedCBSinceSnapshotCache = cbs.flatten))
      )
    }

  def updateAcceptedCBSinceSnapshot(cb: CheckpointBlock): F[Unit] =
    acceptedCBSinceSnapshot.get.flatMap { accepted =>
      if (accepted.contains(cb.baseHash)) {
        dao.metrics.incrementMetricAsync("checkpointAcceptedButAlreadyInAcceptedCBSinceSnapshot")
      } else {
        acceptedCBSinceSnapshot.modify(a => (a :+ cb.baseHash, ())).flatTap { _ =>
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
        acceptedCBSinceSnapshot.modify(accepted => (accepted.slice(0, 100), ())) >>
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
          if (minTipHeight > (nextHeightInterval + snapshotHeightDelayInterval))
            ().asRight[SnapshotError]
          else
            HeightIntervalConditionNotMet.asLeft[Unit]
        }.flatTap { e =>
          if (e.isRight)
            logger.debug(
              s"height interval met minTipHeight: $minTipHeight nextHeightInterval: $nextHeightInterval and ${nextHeightInterval + snapshotHeightDelayInterval}"
            ) >> dao.metrics.incrementMetricAsync[F]("snapshotHeightIntervalConditionMet")
          else
            logger.debug(
              s"height interval not met minTipHeight: $minTipHeight nextHeightInterval: $nextHeightInterval and ${nextHeightInterval + snapshotHeightDelayInterval}"
            ) >> dao.metrics.incrementMetricAsync[F]("snapshotHeightIntervalConditionNotMet")
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
      _ <- logger.debug(
        s"blocks for snapshot between lastSnapshotHeight: $height nextHeightInterval: $nextHeightInterval"
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

  private def getNextSnapshot(hashesForNextSnapshot: Seq[String], publicReputation: Map[Id, Double]): F[Snapshot] =
    storedSnapshot.get
      .map(_.snapshot.hash)
      .map(hash => Snapshot(hash, hashesForNextSnapshot, publicReputation))

  private[storage] def applySnapshot()(implicit C: ContextShift[F]): EitherT[F, SnapshotError, Unit] = {
    val write: Snapshot => EitherT[F, SnapshotError, Unit] = (currentSnapshot: Snapshot) =>
      for {
        _ <- writeSnapshotToDisk(currentSnapshot)
        _ <- writeSnapshotInfoToDisk()
        _ <- applyAfterSnapshot(currentSnapshot)
      } yield ()

    storedSnapshot.get.attemptT
      .leftMap(SnapshotUnexpectedError)
      .map(_.snapshot)
      .flatMap { currentSnapshot =>
        if (currentSnapshot == Snapshot.snapshotZero) EitherT.rightT[F, SnapshotError](())
        else write(currentSnapshot)
      }
  }

  def addSnapshotToDisk(snapshot: StoredSnapshot): EitherT[F, Throwable, Unit] =
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
              maybeCache => maybeCache._2.isEmpty || maybeCache._2.isEmpty
            ) =>
          EitherT {
            Sync[F].delay {
              maybeBlocks.find(maybeCache => maybeCache._2.isEmpty || maybeCache._2.isEmpty)
            }.flatTap { maybeEmpty =>
              logger.error(s"Snapshot data is missing for block: ${maybeEmpty}")
            }.flatTap(_ => dao.metrics.incrementMetricAsync("snapshotInvalidData"))
              .map(_ => Left(SnapshotIllegalState))
          }

        case maybeBlocks =>
          val flatten = maybeBlocks.flatMap(_._2).sortBy(_.checkpointBlock.baseHash)
          Snapshot
            .writeSnapshot(StoredSnapshot(currentSnapshot, flatten))
            .biSemiflatMap(
              t =>
                dao.metrics
                  .incrementMetricAsync(Metrics.snapshotWriteToDisk + Metrics.failure)
                  .map { _ =>
                    logger.debug("t.getStackTrace: " + t.getStackTrace)
                    SnapshotIOError(t)
                  },
              _ =>
                logger
                  .debug(s"Snapshot written: ${currentSnapshot.hash}")
                  .flatMap(_ => dao.metrics.incrementMetricAsync(Metrics.snapshotWriteToDisk + Metrics.success))
            )
      }

  private def applyAfterSnapshot(currentSnapshot: Snapshot): EitherT[F, SnapshotError, Unit] = {
    val applyAfter = for {
      _ <- acceptSnapshot(currentSnapshot)

      _ <- totalNumCBsInSnapshots.modify(t => (t + currentSnapshot.checkpointBlocks.size, ()))
      _ <- totalNumCBsInSnapshots.get.flatTap { total =>
        dao.metrics.updateMetricAsync("totalNumCBsInShapshots", total.toString)
      }

      soeHashes <- getSOEHashesFrom(currentSnapshot.checkpointBlocks.toList)
      _ <- checkpointService.applySnapshot(currentSnapshot.checkpointBlocks.toList)
      _ <- soeService.applySnapshot(soeHashes)
      _ <- logger.info(s"Removed soeHashes : $soeHashes")
      _ <- dao.metrics.updateMetricAsync(Metrics.lastSnapshotHash, currentSnapshot.hash)
      _ <- dao.metrics.incrementMetricAsync(Metrics.snapshotCount)
    } yield ()

    applyAfter.attemptT
      .leftMap(SnapshotUnexpectedError)
  }

  private def getSOEHashesFrom(cbs: List[String]): F[List[String]] =
    cbs
      .traverse(checkpointService.lookup)
      .map(_.flatMap(_.map(_.checkpointBlock.soeHash)))

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
      _ <- applySnapshotObservations(cbs)
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

  private def applySnapshotObservations(cbs: List[CheckpointBlockMetadata]): F[Unit] =
    for {
      _ <- cbs.traverse(c => c.observationsMerkleRoot.traverse(observationService.applySnapshot)).void
    } yield ()

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
    LiftIO[F].liftIO(dao.leavingPeers.flatMap(_.values.toList.traverse(dao.cluster.removePeer))).void
}

object SnapshotService {

  def apply[F[_]: Concurrent](
    concurrentTipService: ConcurrentTipService[F],
    cloudStorage: CloudStorage[F],
    addressService: AddressService[F],
    checkpointService: CheckpointService[F],
    messageService: MessageService[F],
    transactionService: TransactionService[F],
    observationService: ObservationService[F],
    rateLimiting: RateLimiting[F],
    consensusManager: ConsensusManager[F],
    trustManager: TrustManager[F],
    soeService: SOEService[F],
    snapshotStorage: SnapshotStorage[F],
    rewardsManager: RewardsManager[F],
    dao: DAO
  )(implicit C: ContextShift[F], P: Parallel[F]) =
    new SnapshotService[F](
      concurrentTipService,
      cloudStorage,
      addressService,
      checkpointService,
      messageService,
      transactionService,
      observationService,
      rateLimiting,
      consensusManager,
      trustManager,
      soeService,
      rewardsManager,
      snapshotStorage,
      dao
    )
}

sealed trait SnapshotError extends Throwable {
  def message: String
}

object MaxCBHashesInMemory extends SnapshotError {
  def message: String = "Reached maximum checkpoint block hashes in memory"
}

object NodeNotReadyForSnapshots extends SnapshotError {
  def message: String = "Node is not ready for creating snapshots"
}

object NoAcceptedCBsSinceSnapshot extends SnapshotError {
  def message: String = "Node has no checkpoint blocks since last snapshot"
}

object HeightIntervalConditionNotMet extends SnapshotError {
  def message: String = "Height interval condition has not been met"
}

object NoBlocksWithinHeightInterval extends SnapshotError {
  def message: String = "Found no blocks within the next snapshot height interval"
}

object SnapshotIllegalState extends SnapshotError {
  def message: String = "Snapshot illegal state"
}

case class SnapshotIOError(cause: Throwable) extends SnapshotError {
  def message: String = s"Snapshot IO error: ${cause.getMessage}"
}
case class SnapshotUnexpectedError(cause: Throwable) extends SnapshotError {
  def message: String = s"Snapshot unexpected error: ${cause.getMessage}"
}

case class SnapshotInfoIOError(cause: Throwable) extends SnapshotError {
  def message: String = s"SnapshotInfo IO error: ${cause.getMessage}"
}

case class SnapshotCreated(hash: String, height: Long, publicReputation: Map[Id, Double])
