package org.constellation.storage

import cats.Parallel
import cats.data.EitherT
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, LiftIO, Sync}
import cats.syntax.all._
import constellation.withMetric
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.CheckpointService
import org.constellation.consensus._
import org.constellation.domain.cloud.CloudStorageOld
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.rewards.StoredRewards
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.domain.transaction.TransactionService
import org.constellation.p2p.Cluster
import org.constellation.schema.checkpoint.{
  CheckpointBlock,
  CheckpointBlockMetadata,
  CheckpointCache,
  FinishedCheckpoint
}
import org.constellation.schema.{Id, NodeState}
import org.constellation.rewards.EigenTrust
import org.constellation.schema.snapshot.{Snapshot, SnapshotInfo, StoredSnapshot, TotalSupply}
import org.constellation.schema.transaction.TransactionCacheData
import org.constellation.serialization.KryoSerializer
import org.constellation.trust.TrustManager
import org.constellation.util.Metrics
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

import scala.collection.SortedMap
import scala.concurrent.ExecutionContext

class SnapshotService[F[_]: Concurrent](
  concurrentTipService: ConcurrentTipService[F],
  cloudStorage: CloudStorageOld[F],
  addressService: AddressService[F],
  checkpointService: CheckpointService[F],
  messageService: MessageService[F],
  transactionService: TransactionService[F],
  observationService: ObservationService[F],
  rateLimiting: RateLimiting[F],
  consensusManager: ConsensusManager[F],
  trustManager: TrustManager[F],
  soeService: SOEService[F],
  snapshotStorage: LocalFileStorage[F, StoredSnapshot],
  snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
  eigenTrustStorage: LocalFileStorage[F, StoredRewards],
  eigenTrust: EigenTrust[F],
  dao: DAO,
  boundedExecutionContext: ExecutionContext,
  unboundedExecutionContext: ExecutionContext
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
  val nextSnapshotHash: Ref[F, String] = Ref.unsafe("")

  def exists(hash: String): F[Boolean] =
    for {
      last <- storedSnapshot.get
      hashes <- snapshotStorage.list().rethrowT
    } yield last.snapshot.hash == hash || hashes.contains(hash)

  def isStored(hash: String): F[Boolean] =
    snapshotStorage.exists(hash)

  def getLastSnapshotHeight: F[Int] = lastSnapshotHeight.get

  def getAcceptedCBSinceSnapshot: F[Seq[String]] =
    for {
      hashes <- acceptedCBSinceSnapshot.get
    } yield hashes

  def attemptSnapshot()(implicit cluster: Cluster[F]): EitherT[F, SnapshotError, SnapshotCreated] =
    for {
      _ <- checkDiskSpace()

      _ <- validateMaxAcceptedCBHashesInMemory()
      _ <- validateAcceptedCBsSinceSnapshot()

      nextHeightInterval <- getNextHeightInterval.attemptT.leftMap(SnapshotUnexpectedError).leftWiden[SnapshotError]
      minActiveTipHeight <- LiftIO[F]
        .liftIO(dao.getActiveMinHeight)
        .attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      minTipHeight <- concurrentTipService
        .getMinTipHeight(minActiveTipHeight)
        .attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      _ <- validateSnapshotHeightIntervalCondition(nextHeightInterval, minTipHeight)
      blocksWithinHeightInterval <- getBlocksWithinHeightInterval(nextHeightInterval).attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      _ <- validateBlocksWithinHeightInterval(blocksWithinHeightInterval)
      allBlocks = blocksWithinHeightInterval.map(_.get).sortBy(_.checkpointBlock.baseHash)

      hashesForNextSnapshot = allBlocks.map(_.checkpointBlock.baseHash)
      publicReputation <- trustManager.getPredictedReputation.attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      nextSnapshot <- getNextSnapshot(hashesForNextSnapshot, publicReputation).attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      _ <- nextSnapshotHash
        .modify(_ => (nextSnapshot.hash, ()))
        .attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]

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
      _ <- C
        .evalOn(boundedExecutionContext)(applySnapshot().rethrowT)
        .attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      _ <- lastSnapshotHeight
        .set(nextHeightInterval.toInt)
        .attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      _ <- acceptedCBSinceSnapshot
        .update(_.filterNot(hashesForNextSnapshot.contains))
        .attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      _ <- calculateAcceptedTransactionsSinceSnapshot().attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      _ <- updateMetricsAfterSnapshot().attemptT.leftMap(SnapshotUnexpectedError).leftWiden[SnapshotError]

      snapshot = StoredSnapshot(nextSnapshot, allBlocks)
      _ <- storedSnapshot.set(snapshot).attemptT.leftMap(SnapshotUnexpectedError).leftWiden[SnapshotError]
      // TODO: pass stored snapshot to writeSnapshotToDisk
      _ <- writeSnapshotToDisk(snapshot.snapshot)
      _ <- writeSnapshotInfoToDisk()
      // For now we do not restore EigenTrust model
      //      _ <- writeEigenTrustToDisk(snapshot.snapshot)

      _ <- markLeavingPeersAsOffline().attemptT.leftMap(SnapshotUnexpectedError).leftWiden[SnapshotError]
      _ <- removeOfflinePeers().attemptT.leftMap(SnapshotUnexpectedError).leftWiden[SnapshotError]

      created = SnapshotCreated(
        nextSnapshot.hash,
        nextHeightInterval,
        publicReputation
      )
    } yield created

  // TODO
  //_ <- if (ConfigUtil.isEnabledCloudStorage) cloudStorage.upload(Seq(File(path))).void else Sync[F].unit

  def writeSnapshotInfoToDisk(): EitherT[F, SnapshotInfoIOError, Unit] =
    getSnapshotInfoWithFullData.attemptT.flatMap { info =>
      val hash = info.snapshot.snapshot.hash

      if (info.snapshot.snapshot == Snapshot.snapshotZero) {
        EitherT.liftF[F, Throwable, Unit](Sync[F].unit)
      } else {
        C.evalOn(boundedExecutionContext)(Sync[F].delay { KryoSerializer.serializeAnyRef(info) }).attemptT.flatMap {
          snapshotInfoStorage.write(hash, _)
        }
      }
    }.leftMap(SnapshotInfoIOError)

  def writeEigenTrustToDisk(snapshot: Snapshot): EitherT[F, EigenTrustIOError, Unit] =
    (for {
      agents <- eigenTrust.getAgents().attemptT
      model <- eigenTrust.getModel().attemptT
      storedEigenTrust = StoredRewards(agents, model)
      _ <- eigenTrustStorage.write(snapshot.hash, KryoSerializer.serializeAnyRef(storedEigenTrust))
    } yield ()).leftMap(EigenTrustIOError)

  def getSnapshotInfo(): F[SnapshotInfo] =
    for {
      s <- storedSnapshot.get
      accepted <- acceptedCBSinceSnapshot.get
      lastHeight <- lastSnapshotHeight.get
      hashes <- snapshotStorage.list().rethrowT
      addressCacheData <- addressService.getAll
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

  def getTotalSupply(): F[TotalSupply] =
    for {
      snapshotInfo <- getSnapshotInfo()
      height = snapshotInfo.snapshot.height
      totalSupply = snapshotInfo.addressCacheData.values.map(_.balanceByLatestSnapshot).sum
    } yield TotalSupply(height, totalSupply)

  def setSnapshot(snapshotInfo: SnapshotInfo): F[Unit] =
    for {
      _ <- C.evalOn(boundedExecutionContext)(removeStoredSnapshotDataFromMempool())
      _ <- storedSnapshot.modify(_ => (snapshotInfo.snapshot, ()))
      _ <- lastSnapshotHeight.modify(_ => (snapshotInfo.lastSnapshotHeight, ()))
      _ <- LiftIO[F].liftIO(dao.checkpointAcceptanceService.awaiting.modify(_ => (snapshotInfo.awaitingCbs, ())))
      _ <- concurrentTipService.set(snapshotInfo.tips)
      _ <- acceptedCBSinceSnapshot.modify(_ => (snapshotInfo.acceptedCBSinceSnapshot, ()))
      _ <- transactionService.transactionChainService.applySnapshotInfo(snapshotInfo)
      _ <- C.evalOn(boundedExecutionContext)(addressService.setAll(snapshotInfo.addressCacheData))
      _ <- C.evalOn(boundedExecutionContext) {
        (snapshotInfo.snapshotCache ++ snapshotInfo.acceptedCBSinceSnapshotCache).toList.traverse { h =>
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

  def getLocalAcceptedCBSinceSnapshotCache(snapHashes: Seq[String]): F[List[CheckpointCache]] =
    snapHashes.toList.traverse(str => checkpointService.fullData(str)).map(lstOpts => lstOpts.flatten)

  def getCheckpointAcceptanceService = LiftIO[F].liftIO(dao.checkpointAcceptanceService.awaiting.get)

  def removeStoredSnapshotDataFromMempool(): F[Unit] =
    for {
      snap <- storedSnapshot.get
      accepted <- acceptedCBSinceSnapshot.get
      cbs = (snap.snapshot.checkpointBlocks ++ accepted).toList
      fetched <- getCheckpointBlocksFromSnapshot(cbs)
      _ <- fetched.traverse(_.transactionsMerkleRoot.traverse(transactionService.removeMerkleRoot))
      _ <- fetched.traverse(_.observationsMerkleRoot.traverse(observationService.removeMerkleRoot))
      soeHashes <- getSOEHashesFrom(cbs)
      _ <- checkpointService.batchRemove(cbs)
      _ <- soeService.batchRemove(soeHashes)
      _ <- logger.info(s"Removed soeHashes : $soeHashes")
    } yield ()

  def syncBufferAccept(cb: FinishedCheckpoint): F[Unit] =
    for {
      size <- syncBuffer.modify { curr =>
        val updated = curr + (cb.checkpointCacheData.checkpointBlock.baseHash -> cb)
        (updated, updated.size)
      }
      _ <- dao.metrics.updateMetricAsync[F]("syncBufferSize", size)
    } yield ()

  def syncBufferPull(): F[Map[String, FinishedCheckpoint]] =
    for {
      pulled <- syncBuffer.modify(curr => (Map.empty, curr))
      _ <- dao.metrics.updateMetricAsync[F]("syncBufferSize", pulled.size)
    } yield pulled

  def getSnapshotInfoWithFullData: F[SnapshotInfo] =
    getSnapshotInfo().flatMap { info =>
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

  private def checkDiskSpace(): EitherT[F, SnapshotError, Unit] = EitherT {
    snapshotStorage.getUsableSpace.map { space =>
      if (space < 1073741824) { // 1Gb in bytes
        NotEnoughSpace.asLeft[Unit]
      } else {
        Right(())
      }
    }
  }

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

  private def getNextSnapshot(
    hashesForNextSnapshot: Seq[String],
    publicReputation: Map[Id, Double]
  ): F[Snapshot] =
    storedSnapshot.get
      .map(_.snapshot.hash)
      .map(hash => Snapshot(hash, hashesForNextSnapshot, SortedMap(publicReputation.toSeq: _*)))

  private[storage] def applySnapshot()(implicit C: ContextShift[F]): EitherT[F, SnapshotError, Unit] = {
    val write: Snapshot => EitherT[F, SnapshotError, Unit] = (currentSnapshot: Snapshot) =>
      applyAfterSnapshot(currentSnapshot)

    storedSnapshot.get.attemptT
      .leftMap(SnapshotUnexpectedError)
      .map(_.snapshot)
      .flatMap { currentSnapshot =>
        if (currentSnapshot == Snapshot.snapshotZero) EitherT.rightT[F, SnapshotError](())
        else write(currentSnapshot)
      }
  }

  def addSnapshotToDisk(snapshot: StoredSnapshot): EitherT[F, Throwable, Unit] =
    for {
      serialized <- EitherT(
        C.evalOn(boundedExecutionContext)(Sync[F].delay(KryoSerializer.serializeAnyRef(snapshot))).attempt
      )
      write <- EitherT(
        C.evalOn(unboundedExecutionContext)(writeSnapshot(snapshot, serialized).value)
      )
    } yield write

  def isOverDiskCapacity(bytesLengthToAdd: Long): F[Boolean] = {
    val sizeDiskLimit = 0 // ConfigUtil.snapshotSizeDiskLimit TODO: check if it works
    if (sizeDiskLimit == 0) return false.pure[F]

    val isOver = for {
      occupiedSpace <- snapshotStorage.getOccupiedSpace
      usableSpace <- snapshotStorage.getUsableSpace
      isOverSpace = occupiedSpace + bytesLengthToAdd > sizeDiskLimit || usableSpace < bytesLengthToAdd
    } yield isOverSpace

    isOver.flatTap { over =>
      if (over) {
        logger.warn(
          s"[${dao.id.short}] isOverDiskCapacity bytes to write ${bytesLengthToAdd} configured space: ${ConfigUtil.snapshotSizeDiskLimit}"
        )
      } else Sync[F].unit
    }
  }

  private def writeSnapshot(
    storedSnapshot: StoredSnapshot,
    serialized: Array[Byte],
    trialNumber: Int = 0
  ): EitherT[F, Throwable, Unit] =
    trialNumber match {
      case x if x >= 3 => EitherT.leftT[F, Unit](new Throwable(s"Unable to write snapshot"))
      case _ =>
        isOverDiskCapacity(serialized.length).attemptT.flatMap { isOver =>
          if (isOver) {
            EitherT.leftT[F, Unit](new Throwable(s"Unable to write snapshot, not enough space"))
          } else {
            withMetric(
              snapshotStorage
                .write(storedSnapshot.snapshot.hash, serialized)
                .value
                .flatMap(Sync[F].fromEither),
              "writeSnapshot"
            ).attemptT
          }
        }
    }

  def writeSnapshotToDisk(
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
          addSnapshotToDisk(StoredSnapshot(currentSnapshot, flatten))
            .biSemiflatMap(
              t =>
                dao.metrics
                  .incrementMetricAsync(Metrics.snapshotWriteToDisk + Metrics.failure)
                  .flatTap(_ => logger.debug("t.getStackTrace: " + t.getStackTrace.mkString("Array(", ", ", ")")))
                  .map(_ => SnapshotIOError(t)),
              _ =>
                logger
                  .debug(s"Snapshot written: ${currentSnapshot.hash}")
                  .flatMap(_ => dao.metrics.incrementMetricAsync(Metrics.snapshotWriteToDisk + Metrics.success))
            )
      }

  private def applyAfterSnapshot(currentSnapshot: Snapshot): EitherT[F, SnapshotError, Unit] = {
    val applyAfter = for {
      _ <- C.evalOn(boundedExecutionContext)(acceptSnapshot(currentSnapshot))

      _ <- totalNumCBsInSnapshots.modify(t => (t + currentSnapshot.checkpointBlocks.size, ()))
      _ <- totalNumCBsInSnapshots.get.flatTap { total =>
        dao.metrics.updateMetricAsync("totalNumCBsInShapshots", total.toString)
      }

      soeHashes <- getSOEHashesFrom(currentSnapshot.checkpointBlocks.toList)
      _ <- checkpointService.batchRemove(currentSnapshot.checkpointBlocks.toList)
      _ <- soeService.batchRemove(soeHashes)
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
//      _ <- cbs.traverse(applySnapshotMessages(s, _))
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

  private def applySnapshotObservations(cbs: List[CheckpointBlockMetadata]): F[Unit] =
    for {
      _ <- cbs.traverse(c => c.observationsMerkleRoot.traverse(observationService.removeMerkleRoot)).void
    } yield ()

  private def applySnapshotTransactions(s: Snapshot, cbs: List[CheckpointBlockMetadata]): F[Unit] =
    for {
      txs <- cbs
        .traverse(_.transactionsMerkleRoot.traverse(checkpointService.fetchBatchTransactions).map(_.getOrElse(List())))
        .map(_.flatten)

      _ <- txs
        .filterNot(_.isDummy)
        .traverse(t => addressService.transferSnapshotTransaction(t))

      _ <- cbs.traverse(
        _.transactionsMerkleRoot.traverse(transactionService.applySnapshot(txs.map(TransactionCacheData(_)), _))
      )
    } yield ()

  private def markLeavingPeersAsOffline(): F[Unit] =
    LiftIO[F]
      .liftIO(dao.leavingPeers)
      .flatMap {
        _.values.toList.map(_.peerMetadata.id).traverse { p =>
          LiftIO[F]
            .liftIO(dao.cluster.markOfflinePeer(p))
            .handleErrorWith(err => logger.warn(err)(s"Cannot mark leaving peer as offline: ${err.getMessage}"))
        }
      }
      .void

  private def removeOfflinePeers(): F[Unit] =
    LiftIO[F]
      .liftIO(dao.cluster.getPeerInfo)
      .map(_.filter {
        case (id, pd) => NodeState.offlineStates.contains(pd.peerMetadata.nodeState)
      })
      .flatMap {
        _.values.toList.traverse { p =>
          LiftIO[F]
            .liftIO(dao.cluster.removePeer(p))
            .handleErrorWith(err => logger.warn(err)(s"Cannot remove offline peer: ${err.getMessage}"))
        }
      }
      .void
}

object SnapshotService {

  def apply[F[_]: Concurrent](
    concurrentTipService: ConcurrentTipService[F],
    cloudStorage: CloudStorageOld[F],
    addressService: AddressService[F],
    checkpointService: CheckpointService[F],
    messageService: MessageService[F],
    transactionService: TransactionService[F],
    observationService: ObservationService[F],
    rateLimiting: RateLimiting[F],
    consensusManager: ConsensusManager[F],
    trustManager: TrustManager[F],
    soeService: SOEService[F],
    snapshotStorage: LocalFileStorage[F, StoredSnapshot],
    snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
    eigenTrustStorage: LocalFileStorage[F, StoredRewards],
    eigenTrust: EigenTrust[F],
    dao: DAO,
    boundedExecutionContext: ExecutionContext,
    unboundedExecutionContext: ExecutionContext
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
      snapshotStorage,
      snapshotInfoStorage,
      eigenTrustStorage,
      eigenTrust,
      dao,
      boundedExecutionContext,
      unboundedExecutionContext
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

object NotEnoughSpace extends SnapshotError {
  def message: String = "Not enough space left on device"
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

case class EigenTrustIOError(cause: Throwable) extends SnapshotError {
  def message: String = s"EigenTrust IO error: ${cause.getMessage}"
}

case class SnapshotCreated(hash: String, height: Long, publicReputation: Map[Id, Double])
