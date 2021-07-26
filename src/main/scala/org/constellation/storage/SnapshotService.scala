package org.constellation.storage

import cats.Parallel
import cats.data.EitherT
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.syntax.all._
import constellation.withMetric
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.constellation.consensus._
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.healthcheck.HealthCheckLoggingHelper.{logIdShort, logIds}
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.redownload.RedownloadStorageAlgebra
import org.constellation.domain.rewards.StoredRewards
import org.constellation.domain.snapshot.SnapshotStorageAlgebra
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.domain.transaction.TransactionService
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.p2p.DataResolver.DataResolverCheckpointsEnqueue
import org.constellation.p2p.{Cluster, MajorityHeight}
import org.constellation.rewards.EigenTrust
import org.constellation.schema.Id
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache}
import org.constellation.schema.observation.{NodeMemberOfActivePool, NodeNotMemberOfActivePool, Observation}
import org.constellation.schema.snapshot.{NextActiveNodes, Snapshot, SnapshotInfo, StoredSnapshot, TotalSupply}
import org.constellation.schema.transaction.TransactionCacheData
import org.constellation.serialization.KryoSerializer
import org.constellation.trust.TrustManager
import org.constellation.util.Metrics
import org.constellation.{ConfigUtil, ProcessingConfig}

import java.security.KeyPair
import scala.collection.SortedMap
import scala.concurrent.ExecutionContext

class SnapshotService[F[_]: Concurrent](
  addressService: AddressService[F],
  checkpointStorage: CheckpointStorageAlgebra[F],
  snapshotServiceStorage: SnapshotStorageAlgebra[F],
  transactionService: TransactionService[F],
  observationService: ObservationService[F],
  rateLimiting: RateLimiting[F],
  consensusManager: ConsensusManager[F],
  trustManager: TrustManager[F],
  snapshotStorage: LocalFileStorage[F, StoredSnapshot],
  snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
  clusterStorage: ClusterStorageAlgebra[F],
  eigenTrustStorage: LocalFileStorage[F, StoredRewards],
  eigenTrust: EigenTrust[F],
  redownloadStorage: RedownloadStorageAlgebra[F],
  checkpointsQueueInstance: DataResolverCheckpointsEnqueue[F],
  boundedExecutionContext: ExecutionContext,
  unboundedExecutionContext: ExecutionContext,
  metrics: Metrics,
  processingConfig: ProcessingConfig,
  nodeId: Id,
  keyPair: KeyPair,
  nodeConfig: NodeConfig
)(implicit C: ContextShift[F], P: Parallel[F]) {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")
  val snapshotHeightDelayInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightDelayInterval")
  val distanceFromMajority: Int = ConfigUtil.constellation.getInt("snapshot.distanceFromMajority")
  val activePeersRotationInterval: Int = ConfigUtil.constellation.getInt("snapshot.activePeersRotationInterval")
  val activePeersRotationEveryNHeights: Int = snapshotHeightInterval * activePeersRotationInterval

  def getNextSnapshotFacilitators: F[NextActiveNodes] =
    snapshotServiceStorage.getStoredSnapshot
      .map(_.snapshot.nextActiveNodes)

  def attemptSnapshot(): EitherT[F, SnapshotError, SnapshotCreated] =
    for {
      lastStoredSnapshot <- snapshotServiceStorage.getStoredSnapshot.attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
      _ <- checkFullActivePeersPoolMembership(lastStoredSnapshot)
      _ <- checkActiveBetweenHeightsCondition()
      _ <- checkDiskSpace()
//      _ <- validateMaxAcceptedCBHashesInMemory()

      nextHeightInterval <- getNextHeightInterval.attemptT.leftMap[SnapshotError](SnapshotUnexpectedError)

      _ <- validateMaxDistanceFromMajority(nextHeightInterval)

      minTipHeight <- checkpointStorage.getMinTipHeight.attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)

      minWaitingHeight <- checkpointStorage.getMinWaitingHeight.attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)

      _ <- validateSnapshotHeightIntervalCondition(nextHeightInterval, minTipHeight, minWaitingHeight)

      blocksWithinHeightInterval <- getBlocksWithinHeightInterval(nextHeightInterval).attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)

      _ <- validateAcceptedCBsSinceSnapshot(blocksWithinHeightInterval.size)

      _ <- validateBlocksWithinHeightInterval(blocksWithinHeightInterval)
      allBlocks = blocksWithinHeightInterval.sortBy(_.checkpointBlock.soeHash)

      hashesForNextSnapshot = allBlocks.map(_.checkpointBlock.soeHash)
      hashesWithHeightForNextSnapshot = hashesForNextSnapshot.map((_, nextHeightInterval))
      publicReputation <- trustManager.getPredictedReputation.attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
      publicReputationString = publicReputation.map { case (id, rep) => logIdShort(id) + " -> " + rep }.toList.toString
      _ <- logger
        .debug(s"Snapshot attempt current reputation: $publicReputationString")
        .attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
      _ <- metrics
        .updateMetricAsync("currentSnapshotReputation", publicReputationString)
        .attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)

      // should trustManager generate the facilitators below?
      nextActiveAndAuthorizedNodes <- calculateNextActiveNodes(publicReputation, nextHeightInterval, lastStoredSnapshot).attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
      (nextActiveNodes, authorizedPeers) = nextActiveAndAuthorizedNodes
      nextSnapshot <- getNextSnapshot(
        hashesForNextSnapshot,
        publicReputation,
        nextActiveNodes,
        authorizedPeers
      ).attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
      _ <- snapshotServiceStorage
        .setNextSnapshotHash(nextSnapshot.hash)
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
      _ <- snapshotServiceStorage
        .setLastSnapshotHeight(nextHeightInterval.toInt)
        .attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      _ <- checkpointStorage
        .markInSnapshot(hashesWithHeightForNextSnapshot.toSet)
        .attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      _ <- updateMetricsAfterSnapshot().attemptT.leftMap(SnapshotUnexpectedError).leftWiden[SnapshotError]

      _ <- rateLimiting
        .reset(hashesForNextSnapshot)(checkpointStorage)
        .attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]

      snapshot = StoredSnapshot(nextSnapshot, allBlocks)
      _ <- snapshotServiceStorage
        .setStoredSnapshot(snapshot)
        .attemptT
        .leftMap(SnapshotUnexpectedError)
        .leftWiden[SnapshotError]
      // TODO: pass stored snapshot to writeSnapshotToDisk
      _ <- writeSnapshotToDisk(snapshot.snapshot)
      _ <- writeSnapshotInfoToDisk().leftWiden[SnapshotError]
      // For now we do not restore EigenTrust model
      //      _ <- writeEigenTrustToDisk(snapshot.snapshot)

      created = SnapshotCreated(
        nextSnapshot.hash,
        nextHeightInterval,
        publicReputation
      )
      activeFullNodes <- clusterStorage
        .getActiveFullPeersIds(true)
        .attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
      activeLightNodes <- clusterStorage
        .getActiveLightPeersIds(true) //TODO: withSelfId not necessary as Light node will never attemptSnapshot unless it's a Full node???
        .attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
      activePeers = activeFullNodes ++ activeLightNodes
      inactivePeers <- clusterStorage.getPeers // TODO: take only nodes that successfully sent the Join Cluster Observation?
        .map(_.keySet -- activePeers)
        .attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
      _ <- sendActivePoolObservations(activePeers = activePeers, inactivePeers = inactivePeers)
        .attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
    } yield created

  // TODO
  //_ <- if (ConfigUtil.isEnabledCloudStorage) cloudStorage.upload(Seq(File(path))).void else Sync[F].unit

  private def calculateNextActiveNodes(
    publicReputation: Map[Id, Double],
    nextHeightInterval: Long,
    lastStoredSnapshot: StoredSnapshot
  ): F[(NextActiveNodes, Set[Id])] =
    for {
      fullNodes <- clusterStorage.getFullPeersIds()
      lightNodes <- clusterStorage.getLightPeersIds()
      authorizedPeers <- clusterStorage.getAuthorizedPeers
      nextActiveNodes = if (nextHeightInterval % activePeersRotationEveryNHeights == 0) {
        val nextFull = publicReputation
          .filterKeys(p => fullNodes.contains(p) && authorizedPeers.contains(p))
          .toList
          .sortBy { case (_, reputation) => reputation }
          .map(_._1)
          .reverse
          .take(3)
          .toSet

        //val lightCandidates = publicReputation.filterKeys(lightNodes.contains)
        //val nextLight = (if (lightCandidates.size >= 3) lightCandidates else publicReputation).toList
        val nextLight = publicReputation
          .filterKeys(p => lightNodes.contains(p) && authorizedPeers.contains(p))
          .toList
          .sortBy { case (_, reputation) => reputation }
          .map(_._1)
          .reverse
          .take(3)
          .toSet

        NextActiveNodes(light = nextLight, full = nextFull)
      } else if (lastStoredSnapshot.snapshot == Snapshot.snapshotZero)
        NextActiveNodes(light = Set.empty, full = nodeConfig.initialActiveFullNodes)
      else
        lastStoredSnapshot.snapshot.nextActiveNodes
    } yield (nextActiveNodes, authorizedPeers)

  def writeSnapshotInfoToDisk(): EitherT[F, SnapshotInfoIOError, Unit] =
    getSnapshotInfo().attemptT.flatMap { info =>
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
      snapshot <- snapshotServiceStorage.getStoredSnapshot
      lastSnapshotHeight <- snapshotServiceStorage.getLastSnapshotHeight
      nextSnapshotHash <- snapshotServiceStorage.getNextSnapshotHash
      checkpoints <- checkpointStorage.getCheckpoints
      waitingForAcceptance <- checkpointStorage.getWaitingForAcceptance.map(_.map(_.checkpointBlock.soeHash))
      accepted <- checkpointStorage.getAccepted
      awaiting <- checkpointStorage.getAwaiting
      inSnapshot <- checkpointStorage.getInSnapshot
      addressCacheData <- addressService.getAll
      lastAcceptedTransactionRef <- transactionService.transactionChainService.getLastAcceptedTransactionMap()
      tips <- checkpointStorage.getTips
      usages <- checkpointStorage.getUsages
    } yield
      SnapshotInfo(
        snapshot,
        lastSnapshotHeight,
        nextSnapshotHash,
        checkpoints,
        waitingForAcceptance,
        accepted,
        awaiting,
        inSnapshot,
        addressCacheData,
        lastAcceptedTransactionRef,
        tips.map(_._1),
        usages
      )

  def getTotalSupply(): F[TotalSupply] =
    for {
      snapshotInfo <- getSnapshotInfo()
      height = snapshotInfo.snapshot.height
      totalSupply = snapshotInfo.addressCacheData.values.map(_.balanceByLatestSnapshot).sum
    } yield TotalSupply(height, totalSupply)

  def setSnapshot(snapshotInfo: SnapshotInfo): F[Unit] =
    for {
//      _ <- C.evalOn(boundedExecutionContext)(removeStoredSnapshotDataFromMempool())
      _ <- snapshotServiceStorage.setStoredSnapshot(snapshotInfo.snapshot)
      _ <- snapshotServiceStorage.setLastSnapshotHeight(snapshotInfo.lastSnapshotHeight)
      _ <- snapshotServiceStorage.setNextSnapshotHash(snapshotInfo.nextSnapshotHash)
      // TODO: find proper momment in the flow for this update
      _ <- clusterStorage.setAuthorizedPeers(snapshotInfo.snapshot.snapshot.authorizedNodes)
      _ <- checkpointStorage.setCheckpoints(snapshotInfo.checkpoints)
      _ <- checkpointStorage.setWaitingForAcceptance(snapshotInfo.waitingForAcceptance)
      _ <- checkpointStorage.setAccepted(snapshotInfo.accepted)
      _ <- checkpointStorage.setAwaiting(snapshotInfo.awaiting)
      _ <- checkpointStorage.setInSnapshot(snapshotInfo.inSnapshot)
      _ <- checkpointsQueueInstance.incrementIgnoreCounter
      _ <- addressService.setAll(snapshotInfo.addressCacheData)
      _ <- transactionService.transactionChainService.applySnapshotInfo(snapshotInfo)

      _ <- checkpointStorage.setTips(snapshotInfo.tips)
      _ <- checkpointStorage.setUsages(snapshotInfo.usages)

      acceptedBlocks = snapshotInfo.checkpoints.filterKeys(snapshotInfo.accepted.contains).values.toSet

      _ <- transactionService.setAccepted(acceptedBlocks)
      _ <- observationService.setAccepted(acceptedBlocks)

//      _ <- C.evalOn(boundedExecutionContext) {
//        (snapshotInfo.snapshotCache ++ snapshotInfo.acceptedCBSinceSnapshotCache).toList.traverse { h =>
//          checkpointStorage.persistCheckpoint(h) >>
//            checkpointStorage.acceptCheckpoint(h.checkpointBlock.soeHash, h.height)
//          metrics.incrementMetricAsync(Metrics.checkpointAccepted) >>
//            h.checkpointBlock.transactions.toList.traverse { tx =>
//              transactionService.applyAfterRedownload(TransactionCacheData(tx), Some(h))
//            } >>
//            h.checkpointBlock.observations.toList.traverse { obs =>
//              observationService.applyAfterRedownload(obs, Some(h))
//            }
//        }
//      }
//      _ <- metrics.updateMetricAsync[F](
//        "acceptedCBCacheMatchesAcceptedSize",
//        (snapshotInfo.acceptedCBSinceSnapshot.size == snapshotInfo.acceptedCBSinceSnapshotCache.size).toString
//      )
//      _ <- logger.info(
//        s"acceptedCBCacheMatchesAcceptedSize size: ${(snapshotInfo.acceptedCBSinceSnapshot.size == snapshotInfo.acceptedCBSinceSnapshotCache.size).toString}"
//      )
//      _ <- logger.info(
//        s"acceptedCBCacheMatchesAcceptedSize diff: ${snapshotInfo.acceptedCBSinceSnapshot.toList.diff(snapshotInfo.acceptedCBSinceSnapshotCache)}"
//      )
      _ <- updateMetricsAfterSnapshot()
    } yield ()

  def getLocalAcceptedCBSinceSnapshotCache(snapHashes: Seq[String]): F[List[CheckpointCache]] =
    snapHashes.toList.traverse(str => checkpointStorage.getCheckpoint(str)).map(lstOpts => lstOpts.flatten)

  def removeStoredSnapshotDataFromMempool(): F[Unit] =
    for {
      snap <- snapshotServiceStorage.getStoredSnapshot
//      accepted <- snapshotServiceStorage.getAcceptedCheckpointsSinceSnapshot
//      soeHashes = (snap.snapshot.checkpointBlocks ++ accepted).toList
//      fetched <- getCheckpointBlocksFromSnapshot(soeHashes)
//      _ <- fetched.traverse(_.transactionsMerkleRoot.traverse(transactionService.removeMerkleRoot))
//      _ <- fetched.traverse(_.observationsMerkleRoot.traverse(observationService.removeMerkleRoot))
//      _ <- checkpointStorage.removeCheckpoints(soeHashes.toSet)
//      _ <- logger.info(s"Removed soeHashes : $soeHashes")
    } yield ()

//  def getSnapshotInfoWithFullData: F[SnapshotInfo] =
//    getSnapshotInfo().flatMap { info =>
//      info.acceptedCBSinceSnapshot.toList
//        .traverse(checkpointStorage.getCheckpoint)
//        .map(cbs => info.copy(acceptedCBSinceSnapshotCache = cbs.flatten))
//    }

  def calculateAcceptedTransactionsSinceSnapshot(): F[Unit] =
    for {
      _ <- Sync[F].unit
//      cbHashes <- snapshotServiceStorage.getAcceptedCheckpointsSinceSnapshot.map(_.toList)
//      _ <- rateLimiting.reset(cbHashes)(checkpointStorage)
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
    checkpointStorage.getAccepted.map(_.size).map { size =>
      if (size > processingConfig.maxAcceptedCBHashesInMemory)
        Left(MaxCBHashesInMemory)
      else
        Right(())
    }
  }

  private def validateAcceptedCBsSinceSnapshot(blocks: Int): EitherT[F, SnapshotError, Unit] = EitherT {
    Sync[F].delay {
      if (blocks == 0) {
        NoAcceptedCBsSinceSnapshot.asLeft[Unit]
      } else ().asRight[SnapshotError]
    }
  }

  private def validateMaxDistanceFromMajority(nextHeightInterval: Long): EitherT[F, SnapshotError, Unit] =
    EitherT {
      redownloadStorage.getLatestMajorityHeight.map { height =>
        nextHeightInterval <= (height + distanceFromMajority)
      }.ifM(
        ().asRight[SnapshotError].pure[F],
        SnapshotUnexpectedError(new Throwable(s"Max distance from majority has been reached!"))
          .asLeft[Unit]
          .leftWiden[SnapshotError]
          .pure[F]
      )
    }

  private def validateSnapshotHeightIntervalCondition(
    nextHeightInterval: Long,
    minTipHeight: Long,
    minWaitingHeight: Option[Long]
  ): EitherT[F, SnapshotError, Unit] =
    EitherT {

      metrics.updateMetricAsync[F]("minTipHeight", minTipHeight) >>
        metrics.updateMetricAsync[F]("minWaitingHeight", minWaitingHeight.getOrElse(0L)) >>
        Sync[F].pure {
          if (minTipHeight > (nextHeightInterval + snapshotHeightDelayInterval)) //  && minWaitingHeight.fold(true)(_ > nextHeightInterval)
            ().asRight[SnapshotError]
          else
            HeightIntervalConditionNotMet.asLeft[Unit]
        }.flatTap { e =>
          if (e.isRight)
            logger.debug(
              s"height interval met minTipHeight: $minTipHeight minWaitingHeight: $minWaitingHeight nextHeightInterval: $nextHeightInterval and ${nextHeightInterval + snapshotHeightDelayInterval}"
            ) >> metrics.incrementMetricAsync[F]("snapshotHeightIntervalConditionMet")
          else
            logger.debug(
              s"height interval not met minTipHeight: $minTipHeight minWaitingHeight: $minWaitingHeight nextHeightInterval: $nextHeightInterval and ${nextHeightInterval + snapshotHeightDelayInterval}"
            ) >> metrics.incrementMetricAsync[F]("snapshotHeightIntervalConditionNotMet")
        }
    }

  def getNextHeightInterval: F[Long] =
    snapshotServiceStorage.getLastSnapshotHeight
      .map(_ + snapshotHeightInterval)

  private def getBlocksWithinHeightInterval(nextHeightInterval: Long): F[List[CheckpointCache]] =
    for {
      height <- snapshotServiceStorage.getLastSnapshotHeight
      soeHashes <- checkpointStorage.getAccepted.map(_.toList)
      blocks <- soeHashes.traverse(checkpointStorage.getCheckpoint).map(_.flatten)
      blocksWithinInterval = blocks.filter { block =>
        block.height.min > height && block.height.min <= nextHeightInterval
      }

      _ <- logger.debug(
        s"blocks for snapshot between lastSnapshotHeight: $height nextHeightInterval: $nextHeightInterval"
      )
    } yield blocksWithinInterval

  private def validateBlocksWithinHeightInterval(
    blocks: List[CheckpointCache]
  ): EitherT[F, SnapshotError, Unit] = EitherT {
    Sync[F].pure {
      if (blocks.isEmpty) {
        Left(NoBlocksWithinHeightInterval)
      } else {
        Right(())
      }
    }.flatMap { e =>
      val tap = if (e.isLeft) {
        metrics.incrementMetricAsync("snapshotNoBlocksWithinHeightInterval")
      } else Sync[F].unit

      tap.map(_ => e)
    }
  }

  private def getNextSnapshot(
    hashesForNextSnapshot: Seq[String],
    publicReputation: Map[Id, Double],
    nextActiveNodes: NextActiveNodes,
    authorizedNodes: Set[Id]
  ): F[Snapshot] =
    snapshotServiceStorage.getStoredSnapshot
      .map(_.snapshot.hash)
      .map(
        hash =>
          Snapshot(
            hash,
            hashesForNextSnapshot,
            SortedMap(publicReputation.toSeq: _*),
            nextActiveNodes,
            authorizedNodes
          )
      )

  private[storage] def applySnapshot()(implicit C: ContextShift[F]): EitherT[F, SnapshotError, Unit] = {
    val write: Snapshot => EitherT[F, SnapshotError, Unit] = (currentSnapshot: Snapshot) =>
      applyAfterSnapshot(currentSnapshot)

    snapshotServiceStorage.getStoredSnapshot.attemptT
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
          s"[${nodeId.short}] isOverDiskCapacity bytes to write ${bytesLengthToAdd} configured space: ${ConfigUtil.snapshotSizeDiskLimit}"
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
            )(metrics).attemptT
          }
        }
    }

  def writeSnapshotToDisk(
    currentSnapshot: Snapshot
  )(implicit C: ContextShift[F]): EitherT[F, SnapshotError, Unit] =
    currentSnapshot.checkpointBlocks.toList
      .traverse(h => checkpointStorage.getCheckpoint(h).map(d => (h, d)))
      .attemptT
      .leftMap(SnapshotUnexpectedError)
      .flatMap {
        case maybeBlocks
            if maybeBlocks.exists(
              // TODO: What is this?
              maybeCache => maybeCache._2.isEmpty || maybeCache._2.isEmpty
            ) =>
          EitherT {
            Sync[F].delay {
              maybeBlocks.find(maybeCache => maybeCache._2.isEmpty || maybeCache._2.isEmpty)
            }.flatTap { maybeEmpty =>
              logger.error(s"Snapshot data is missing for block: ${maybeEmpty}")
            }.flatTap(_ => metrics.incrementMetricAsync("snapshotInvalidData"))
              .map(_ => Left(SnapshotIllegalState))
          }

        case maybeBlocks =>
          val flatten = maybeBlocks.flatMap(_._2).sortBy(_.checkpointBlock.soeHash)
          addSnapshotToDisk(StoredSnapshot(currentSnapshot, flatten))
            .biSemiflatMap(
              t =>
                metrics
                  .incrementMetricAsync(Metrics.snapshotWriteToDisk + Metrics.failure)
                  .flatTap(_ => logger.debug("t.getStackTrace: " + t.getStackTrace.mkString("Array(", ", ", ")")))
                  .map(_ => SnapshotIOError(t)),
              _ =>
                logger
                  .debug(s"Snapshot written: ${currentSnapshot.hash}")
                  .flatMap(_ => metrics.incrementMetricAsync(Metrics.snapshotWriteToDisk + Metrics.success))
            )
      }

  private def applyAfterSnapshot(currentSnapshot: Snapshot): EitherT[F, SnapshotError, Unit] = {
    val applyAfter = for {
      _ <- C.evalOn(boundedExecutionContext)(acceptSnapshot(currentSnapshot))

      soeHashes = currentSnapshot.checkpointBlocks.toSet
//      _ <- checkpointStorage.removeCheckpoints(soeHashes)
//      _ <- logger.info(s"Removed soeHashes : $soeHashes")
      _ <- metrics.updateMetricAsync(Metrics.lastSnapshotHash, currentSnapshot.hash)
      _ <- metrics.incrementMetricAsync(Metrics.snapshotCount)
    } yield ()

    applyAfter.attemptT
      .leftMap(SnapshotUnexpectedError)
  }

  private def updateMetricsAfterSnapshot(): F[Unit] =
    for {
      accepted <- checkpointStorage.getAccepted
      awaiting <- checkpointStorage.getAwaiting
      waitingForAcceptance <- checkpointStorage.getWaitingForAcceptance
      height <- snapshotServiceStorage.getLastSnapshotHeight
      nextHeight = height + snapshotHeightInterval

      _ <- metrics.updateMetricAsync("accepted", accepted.size)
      _ <- metrics.updateMetricAsync("awaiting", awaiting.size)
      _ <- metrics.updateMetricAsync("waitingForAcceptance", waitingForAcceptance.size)
      _ <- metrics.updateMetricAsync("lastSnapshotHeight", height)
      _ <- metrics.updateMetricAsync("nextSnapshotHeight", nextHeight)
    } yield ()

  private def acceptSnapshot(s: Snapshot): F[Unit] =
    for {
      cbs <- getCheckpointBlocksFromSnapshot(s.checkpointBlocks.toList)
      _ <- applySnapshotTransactions(s, cbs)
      _ <- applySnapshotObservations(cbs)
    } yield ()

  private def getCheckpointBlocksFromSnapshot(blocks: List[String]): F[List[CheckpointBlock]] =
    for {
      cbData <- blocks.map(checkpointStorage.getCheckpoint).sequence

      _ <- if (cbData.exists(_.isEmpty)) {
        metrics.incrementMetricAsync("snapshotCBAcceptQueryFailed")
      } else Sync[F].unit

      cbs = cbData.flatten.map(_.checkpointBlock)
    } yield cbs

  private def applySnapshotObservations(cbs: List[CheckpointBlock]): F[Unit] =
    for {
      _ <- cbs.traverse(c => c.observations.toList.traverse(o => observationService.remove(o.hash)))
    } yield ()

  private def applySnapshotTransactions(s: Snapshot, cbs: List[CheckpointBlock]): F[Unit] =
    for {
      txs <- cbs.flatMap(_.transactions.toList).pure[F]
      _ <- txs
        .filterNot(_.isDummy)
        .traverse(t => addressService.transferSnapshotTransaction(t))

      _ <- transactionService.applySnapshotDirect(txs.map(TransactionCacheData(_)))
    } yield ()

  private def isMemberOfFullActivePeersPool(snapshot: Snapshot): Boolean =
    snapshot.nextActiveNodes.full.contains(nodeId) // full or light???

  private def checkFullActivePeersPoolMembership(storedSnapshot: StoredSnapshot): EitherT[F, SnapshotError, Unit] =
    for {
      checkedSnapshot <- if (storedSnapshot.snapshot == Snapshot.snapshotZero)
        EitherT.rightT[F, SnapshotError](
          storedSnapshot.snapshot
            .copy(nextActiveNodes = NextActiveNodes(light = Set.empty, nodeConfig.initialActiveFullNodes))
        )
      else {
        EitherT.rightT[F, SnapshotError](
          storedSnapshot.snapshot
        )
      }
      activeBetweenHeights = Cluster.calculateActiveBetweenHeights(0L, activePeersRotationEveryNHeights)
      _ <- if (storedSnapshot.snapshot == Snapshot.snapshotZero)
        clusterStorage
          .setActivePeers(checkedSnapshot.nextActiveNodes, activeBetweenHeights, metrics)
          .attemptT
          .leftMap[SnapshotError](SnapshotUnexpectedError)
      else
        EitherT.rightT[F, SnapshotError](())

      _ <- if (isMemberOfFullActivePeersPool(checkedSnapshot))
        EitherT.rightT[F, SnapshotError](())
      else
        EitherT.leftT[F, Unit](NodeNotPartOfL0FacilitatorsPool.asInstanceOf[SnapshotError])

    } yield ()

  private def checkActiveBetweenHeightsCondition(): EitherT[F, SnapshotError, Unit] =
    for {
      nextHeight <- getNextHeightInterval.attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
      activeBetweenHeights <- clusterStorage.getActiveBetweenHeights.attemptT
        .leftMap[SnapshotError](SnapshotUnexpectedError)
      result <- if (activeBetweenHeights.joined.forall(_ <= nextHeight) && activeBetweenHeights.left.forall(
                      _ >= nextHeight
                    ))
        EitherT.rightT[F, SnapshotError](())
      else
        EitherT.leftT[F, Unit](ActiveBetweenHeightsConditionNotMet.asInstanceOf[SnapshotError]) //can we do it without asInstanceOf?
    } yield result

  //def getTimeInSeconds(): F[Long] = C.monotonic(SECONDS)

  private def sendActivePoolObservations(activePeers: Set[Id], inactivePeers: Set[Id]): F[Unit] =
    for {
      _ <- logger.debug(s"sending observation for ActivePeers: ${logIds(activePeers)}")
      _ <- logger.debug(s"sending observation for InactivePeers: ${logIds(inactivePeers)}")
      //currentEpoch <- getTimeInSeconds()
      activePeersObservations = activePeers.map { id =>
        Observation.create(id, NodeMemberOfActivePool /*, currentEpoch*/ )(keyPair)
      }
      inactivePeersObservations = inactivePeers.map { id =>
        Observation.create(id, NodeNotMemberOfActivePool /*, currentEpoch*/ )(keyPair)
      }
      observations = activePeersObservations ++ inactivePeersObservations
      //      _ <- (activePeersObservations ++ inactivePeersObservations).toList.traverse { observation =>
      //        trustManager.updateStoredReputation(observation)
      //      }
      _ <- observations.toList.traverse(
        observationService
          .put(_)
          .void
          .handleErrorWith(e => logger.warn(e)("Error during sending active pool membership observations"))
      )
    } yield ()
}

object SnapshotService {

  def apply[F[_]: Concurrent](
    apiClient: ClientInterpreter[F],
    clusterStorage: ClusterStorageAlgebra[F],
    addressService: AddressService[F],
    checkpointStorage: CheckpointStorageAlgebra[F],
    snapshotServiceStorage: SnapshotStorageAlgebra[F],
    transactionService: TransactionService[F],
    observationService: ObservationService[F],
    rateLimiting: RateLimiting[F],
    consensusManager: ConsensusManager[F],
    trustManager: TrustManager[F],
    snapshotStorage: LocalFileStorage[F, StoredSnapshot],
    snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
    eigenTrustStorage: LocalFileStorage[F, StoredRewards],
    eigenTrust: EigenTrust[F],
    redownloadStorage: RedownloadStorageAlgebra[F],
    checkpointsQueueInstance: DataResolverCheckpointsEnqueue[F],
    boundedExecutionContext: ExecutionContext,
    unboundedExecutionContext: ExecutionContext,
    metrics: Metrics,
    processingConfig: ProcessingConfig,
    nodeId: Id,
    keyPair: KeyPair,
    nodeConfig: NodeConfig
  )(implicit C: ContextShift[F], P: Parallel[F]) =
    new SnapshotService[F](
      addressService,
      checkpointStorage,
      snapshotServiceStorage,
      transactionService,
      observationService,
      rateLimiting,
      consensusManager,
      trustManager,
      snapshotStorage,
      snapshotInfoStorage,
      clusterStorage,
      eigenTrustStorage,
      eigenTrust,
      redownloadStorage,
      checkpointsQueueInstance,
      boundedExecutionContext,
      unboundedExecutionContext,
      metrics,
      processingConfig,
      nodeId,
      keyPair,
      nodeConfig
    )
}

case class JoinActivePoolCommand(lastActiveFullNodes: Set[Id], lastActiveBetweenHeight: MajorityHeight)

object JoinActivePoolCommand {
  implicit val joinActivePoolCommandCodec: Codec[JoinActivePoolCommand] = deriveCodec
}

//sealed trait ActivePoolAction
//case object JoinLightPool extends ActivePoolAction
//case object JoinFullPool extends ActivePoolAction
//case object LeaveLightPool extends ActivePoolAction
//case object LeaveFullPool extends ActivePoolAction

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

case object NodeNotPartOfL0FacilitatorsPool extends SnapshotError {
  def message: String = "Node is not a part of L0 facilitators pool at the current snapshot height!"
}

case object ActiveBetweenHeightsConditionNotMet extends SnapshotError {
  def message: String = "Next snapshot height is not between current active heights range on the given node!"
}
