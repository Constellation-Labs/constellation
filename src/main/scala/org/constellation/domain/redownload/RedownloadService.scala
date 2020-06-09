package org.constellation.domain.redownload

import cats.NonEmptyParallel
import cats.data.{EitherT, NonEmptyList}
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import org.constellation.ConfigUtil
import org.constellation.checkpoint.{CheckpointAcceptanceService, TopologicalSort}
import org.constellation.consensus.{FinishedCheckpoint, StoredSnapshot}
import org.constellation.domain.cloud.HeightHashFileStorage
import org.constellation.domain.redownload.MajorityStateChooser.SnapshotProposal
import org.constellation.domain.redownload.RedownloadService._
import org.constellation.domain.rewards.StoredRewards
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.p2p.Cluster
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.domain.storage.FileStorage
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.{Cluster, MajorityHeight}
import org.constellation.primitives.Schema.NodeState
import org.constellation.rewards.RewardsManager
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.storage.SnapshotService
import org.constellation.util.Logging.stringifyStackTrace
import org.constellation.util.Metrics

import scala.collection.SortedMap
import scala.concurrent.duration._
import scala.util.Random

class RedownloadService[F[_]: NonEmptyParallel](
  meaningfulSnapshotsCount: Int,
  redownloadInterval: Int,
  isEnabledCloudStorage: Boolean,
  cluster: Cluster[F],
  majorityStateChooser: MajorityStateChooser,
  snapshotStorage: LocalFileStorage[F, StoredSnapshot],
  snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
  rewardsStorage: LocalFileStorage[F, StoredRewards],
  snapshotService: SnapshotService[F],
  checkpointAcceptanceService: CheckpointAcceptanceService[F],
  snapshotCloudStorage: HeightHashFileStorage[F, StoredSnapshot],
  snapshotInfoCloudStorage: HeightHashFileStorage[F, SnapshotInfo],
  rewardsCloudStorage: HeightHashFileStorage[F, StoredRewards],
  rewardsManager: RewardsManager[F],
  apiClient: ClientInterpreter[F],
  metrics: Metrics
)(implicit F: Concurrent[F], C: ContextShift[F], T: Timer[F]) {

  /**
    * It contains immutable historical data
    * (even incorrect snapshots which have been "fixed" by majority in acceptedSnapshots).
    * It is used as own proposals along with peerProposals to calculate majority state.
    */
  private[redownload] val createdSnapshots: Ref[F, SnapshotProposalsAtHeight] = Ref.unsafe(Map.empty)

  /**
    * Can be modified ("fixed"/"aligned") by redownload process. It stores current state of snapshots after auto-healing.
    */
  private[redownload] val acceptedSnapshots: Ref[F, SnapshotsAtHeight] = Ref.unsafe(Map.empty)

  /**
    * Majority proposals from other peers. It is used to calculate majority state.
    */
  private[redownload] val peersProposals: Ref[F, PeersProposals] = Ref.unsafe(Map.empty)

  private[redownload] val lastMajorityState: Ref[F, SnapshotsAtHeight] = Ref.unsafe(Map.empty)

  private[redownload] var lastSentHeight: Ref[F, Long] = Ref.unsafe(-1L)

  private val logger = Slf4jLogger.getLogger[F]

  def setLastSentHeight(height: Long): F[Unit] =
    lastSentHeight.modify { _ =>
      (height, ())
    } >> metrics.updateMetricAsync[F]("redownload_lastSentHeight", height)

  def setLastMajorityState(majorityState: SnapshotsAtHeight): F[Unit] =
    lastMajorityState.modify { _ =>
      (majorityState, maxHeight(majorityState))
    }.flatMap(metrics.updateMetricAsync[F]("redownload_lastMajorityStateHeight", _))

  def latestMajorityHeight: F[Long] = lastMajorityState.modify { s =>
    (s, maxHeight(s))
  }

  def lowestMajorityHeight: F[Long] = lastMajorityState.modify { s =>
    (s, minHeight(s))
  }

  def updatePeerProposal(peer: Id, proposal: SnapshotProposalsAtHeight): F[Unit] =
    peersProposals.modify { m =>
      val updated = m.updated(peer, proposal)
      (updated, ())
    }

  def persistCreatedSnapshot(height: Long, hash: String, reputation: Reputation): F[Unit] =
    createdSnapshots.modify { m =>
      val updated = if (m.contains(height)) m else m.updated(height, SnapshotProposal(hash, reputation))
      val max = maxHeight(updated)
      val removalPoint = getRemovalPoint(max)
      val limited = takeHighestUntilKey(updated, removalPoint)
      (limited, ())
    } >> metrics.updateMetricAsync[F]("redownload_maxCreatedSnapshotHeight", height)

  def persistAcceptedSnapshot(height: Long, hash: String): F[Unit] =
    acceptedSnapshots.modify { m =>
      val updated = m.updated(height, hash)
      val max = maxHeight(updated)
      val removalPoint = getRemovalPoint(max)
      val limited = takeHighestUntilKey(updated, removalPoint)
      (limited, ())
    } >> metrics.updateMetricAsync[F]("redownload_maxAcceptedSnapshotHeight", height)

  def getCreatedSnapshots(): F[SnapshotProposalsAtHeight] = createdSnapshots.get

  def getAcceptedSnapshots(): F[SnapshotsAtHeight] = acceptedSnapshots.get

  def getPeerProposals(): F[PeersProposals] = peersProposals.get

  def clear: F[Unit] =
    for {
      _ <- createdSnapshots.modify(_ => (Map.empty, ()))
      _ <- acceptedSnapshots.modify(_ => (Map.empty, ()))
      _ <- peersProposals.modify(_ => (Map.empty, ()))
      _ <- setLastMajorityState(Map.empty)
      _ <- setLastSentHeight(-1)
      _ <- rewardsManager.clearLastRewardedHeight()
    } yield ()

  def fetchAndUpdatePeersProposals(): F[PeersProposals] =
    for {
      _ <- logger.debug("Fetching and updating peer proposals")
      peers <- cluster.getPeerInfo.map(
        _.values.toList.filter(p => NodeState.isNotOffline(p.peerMetadata.nodeState))
      )
      apiClients = peers.map(_.peerMetadata.toPeerClientMetadata)
      responses <- apiClients.traverse { client =>
        fetchCreatedSnapshots(client).map(client.id -> _)
      }
      newProposals = responses.toMap
      proposals <- peersProposals.modify { m =>
        val updated = m.map {
          case (id, proposalsAtHeight) =>
            val diff = newProposals.getOrElse(id, Map.empty) -- proposalsAtHeight.keySet

            id -> (proposalsAtHeight ++ diff)
        } ++ (newProposals -- m.keySet)

        val ignored = updated.mapValues(a => takeHighestUntilKey(a, getRemovalPoint(maxHeight(a))))
        (ignored, ignored)
      }
    } yield proposals

  private[redownload] def fetchStoredSnapshotsFromAllPeers(): F[Map[PeerClientMetadata, Set[String]]] =
    for {
      peers <- cluster.getPeerInfo.map(_.values.toList)
      apiClients = peers.map(_.peerMetadata.toPeerClientMetadata)
      responses <- apiClients.traverse { client =>
        fetchStoredSnapshots(client)
          .map(client -> _.toSet)
      }
    } yield responses.toMap

  // TODO: Extract to HTTP layer
  private[redownload] def fetchAcceptedSnapshots(client: PeerClientMetadata): F[SnapshotsAtHeight] =
    apiClient.snapshot
      .getAcceptedSnapshots()
      .run(client)
      .handleErrorWith(_ => F.pure(Map.empty))

  // TODO: Extract to HTTP layer
  private def fetchCreatedSnapshots(client: PeerClientMetadata): F[SnapshotProposalsAtHeight] =
    apiClient.snapshot
      .getCreatedSnapshots()
      .run(client)
      .handleErrorWith { e =>
        logger.error(s"Fetch peers proposals error: ${e.getMessage}") >> F.pure(Map.empty)
      }

  // TODO: Extract to HTTP layer
  private def fetchStoredSnapshots(client: PeerClientMetadata): F[List[String]] =
    apiClient.snapshot
      .getStoredSnapshots()
      .run(client)
      .handleErrorWith(_ => F.pure(List.empty))

  // TODO: Extract to HTTP layer
  private[redownload] def fetchSnapshotInfo(client: PeerClientMetadata): F[SnapshotInfoSerialized] =
    apiClient.snapshot
      .getSnapshotInfo()
      .run(client)

  // TODO: Extract to HTTP layer
  private def fetchSnapshotInfo(hash: String)(client: PeerClientMetadata): F[SnapshotInfoSerialized] =
    apiClient.snapshot
      .getSnapshotInfo(hash)
      .run(client)

  // TODO: Extract to HTTP layer
  private[redownload] def fetchSnapshot(hash: String)(client: PeerClientMetadata): F[SnapshotSerialized] =
    apiClient.snapshot
      .getStoredSnapshot(hash)
      .run(client)

  private def fetchAndStoreMissingSnapshots(
    snapshotsToDownload: SnapshotsAtHeight
  ): EitherT[F, Throwable, Unit] =
    for {
      storedSnapshots <- fetchStoredSnapshotsFromAllPeers().attemptT
      candidates = snapshotsToDownload.values.toList.map { hash =>
        (hash, storedSnapshots.filter { case (_, hashes) => hashes.contains(hash) }.keySet)
      }.toMap

      missingSnapshots <- candidates.toList.traverse {
        case (hash, pool) =>
          useRandomClient(pool) { client =>
            (fetchSnapshot(hash)(client), fetchSnapshotInfo(hash)(client)).parMapN { (snapshot, snapshotInfo) =>
              (hash, (snapshot, snapshotInfo))
            }
          }
      }.attemptT

      _ <- missingSnapshots.traverse {
        case (hash, (snapshot, snapshotInfo)) =>
          snapshotStorage.write(hash, snapshot) >> snapshotInfoStorage.write(hash, snapshotInfo)
      }.void
    } yield ()

  private[redownload] def useRandomClient[A](pool: Set[PeerClientMetadata], attempts: Int = 3)(
    fetchFn: PeerClientMetadata => F[A]
  ): F[A] =
    if (pool.isEmpty) {
      F.raiseError[A](new Throwable("Client pool is empty!"))
    } else {
      val poolArray = pool.toArray
      val stopAt = Random.nextInt(poolArray.length)

      def makeAttempt(index: Int, initialDelay: FiniteDuration = 0.6.seconds, errorsSoFar: Int = 0): F[A] = {
        val client = poolArray(index)
        fetchFn(client).handleErrorWith {
          case e if index == stopAt         => F.raiseError[A](e)
          case _ if errorsSoFar >= attempts => makeAttempt((index + 1) % poolArray.length)
          case _                            => T.sleep(initialDelay) >> makeAttempt(index, initialDelay * 2, errorsSoFar + 1)
        }
      }

      makeAttempt((stopAt + 1) % poolArray.length)
    }

  private def fetchAndPersistBlocksAboveMajority(majorityState: SnapshotsAtHeight): EitherT[F, Throwable, Unit] =
    for {
      storedSnapshots <- fetchStoredSnapshotsFromAllPeers().attemptT

      maxMajorityHash = majorityState(maxHeight(majorityState))
      peersWithMajority = storedSnapshots.filter { case (_, hashes) => hashes.contains(maxMajorityHash) }.keySet

      majoritySnapshotInfo <- snapshotInfoStorage.read(maxMajorityHash)

      _ <- useRandomClient(peersWithMajority) { client =>
        for {
          accepted <- fetchAcceptedSnapshots(client)

          maxMajorityHeight = maxHeight(majorityState)
          acceptedSnapshotHashesAboveMajority = takeHighestUntilKey(accepted, maxMajorityHeight).values

          acceptedSnapshots <- acceptedSnapshotHashesAboveMajority.toList
            .traverse(hash => fetchSnapshot(hash)(client))
            .map(snapshotsSerialized => snapshotsSerialized.map(KryoSerializer.deserializeCast[StoredSnapshot]))

          snapshotInfoFromMemPool <- fetchSnapshotInfo(client)
            .map(KryoSerializer.deserializeCast[SnapshotInfo])
          acceptedBlocksFromSnapshotInfo = snapshotInfoFromMemPool.acceptedCBSinceSnapshotCache.toSet
          awaitingBlocksFromSnapshotInfo = snapshotInfoFromMemPool.awaitingCbs

          blocksFromSnapshots = acceptedSnapshots.flatMap(_.checkpointCache)
          blocksToAccept = (blocksFromSnapshots ++ acceptedBlocksFromSnapshotInfo ++ awaitingBlocksFromSnapshotInfo).distinct

          _ <- snapshotService.setSnapshot(majoritySnapshotInfo)

          _ <- checkpointAcceptanceService.waitingForAcceptance.modify { blocks =>
            val updated = blocks ++ blocksToAccept.map(_.checkpointBlock.soeHash)
            (updated, ())
          }

          _ <- TopologicalSort.sortBlocksTopologically(blocksToAccept).toList.traverse { b =>
            logger.debug(s"Accepting block above majority: ${b.height}") >> checkpointAcceptanceService
              .accept(b)
              .handleErrorWith(
                error => logger.warn(s"Error during blocks acceptance after redownload: ${error.getMessage}") >> F.unit
              )
          }
        } yield ()
      }.attemptT
    } yield ()

  def checkForAlignmentWithMajoritySnapshot(isDownload: Boolean = false): F[Unit] = {
    def wrappedRedownload(
      shouldRedownload: Boolean,
      meaningfulAcceptedSnapshots: SnapshotsAtHeight,
      meaningfulMajorityState: SnapshotsAtHeight
    ) =
      for {
        _ <- if (shouldRedownload) {
          val plan = calculateRedownloadPlan(meaningfulAcceptedSnapshots, meaningfulMajorityState)
          val result = getAlignmentResult(meaningfulAcceptedSnapshots, meaningfulMajorityState, redownloadInterval)
          for {
            _ <- logger.debug(s"Alignment result: $result")
            _ <- logger.debug("Redownload needed! Applying the following redownload plan:")
            _ <- applyRedownloadPlan(plan, meaningfulMajorityState).value.flatMap(F.fromEither)
          } yield ()
        } else logger.debug("No redownload needed - snapshots have been already aligned with majority state. ")
      } yield ()

    val wrappedCheck = for {
      _ <- logger.debug("Checking alignment with majority snapshot...")
      peersProposals <- getPeerProposals()
      createdSnapshots <- getCreatedSnapshots()
      acceptedSnapshots <- getAcceptedSnapshots()

      peers <- cluster.getPeerInfo
      ownPeer <- cluster.getOwnJoinedHeight()
      peersCache = peers.map {
        case (id, peerData) => (id, peerData.majorityHeight)
      } ++ Map(cluster.id -> NonEmptyList.one(MajorityHeight(ownPeer, None)))

      _ <- logger.debug(s"Peers with majority heights")
      _ <- peersCache.toList.traverse {
        case (id, majorityHeight) => logger.debug(s"[$id]: $majorityHeight")
      }

      majorityState = majorityStateChooser.chooseMajorityState(
        createdSnapshots,
        peersProposals,
        peersCache
      )

      maxMajorityHeight = maxHeight(majorityState)
      ignorePoint = getIgnorePoint(maxMajorityHeight)
      meaningfulAcceptedSnapshots = takeHighestUntilKey(acceptedSnapshots, ignorePoint)
      meaningfulMajorityState = takeHighestUntilKey(majorityState, ignorePoint)

      _ <- setLastMajorityState(meaningfulMajorityState)

      _ <- if (meaningfulMajorityState.isEmpty) logger.debug("No majority - skipping redownload") else F.unit

      shouldPerformRedownload = shouldRedownload(
        meaningfulAcceptedSnapshots,
        meaningfulMajorityState,
        redownloadInterval,
        isDownload
      )

      _ <- if (shouldPerformRedownload) {
        cluster.compareAndSet(NodeState.validForRedownload, NodeState.DownloadInProgress).flatMap { state =>
          if (state.isNewSet) {
            wrappedRedownload(shouldPerformRedownload, meaningfulAcceptedSnapshots, meaningfulMajorityState).handleErrorWith {
              error =>
                logger.error(error)(s"Redownload error: ${stringifyStackTrace(error)}") >> cluster
                  .compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
                  .void
            }
          } else F.unit
        }
      } else logger.debug("No redownload needed - snapshots have been already aligned with majority state.")

      _ <- logger.debug("Sending majority snapshots to cloud (if enabled).")
      _ <- if (meaningfulMajorityState.nonEmpty) sendMajoritySnapshotsToCloud()
      else logger.debug("No majority - skipping sending to cloud")

      _ <- logger.debug("Rewarding majority snapshots.")
      _ <- if (meaningfulMajorityState.nonEmpty) rewardMajoritySnapshots().value.flatMap(F.fromEither)
      else logger.debug("No majority - skipping rewarding")

      _ <- logger.debug("Removing unaccepted snapshots from disk.")
      _ <- removeUnacceptedSnapshotsFromDisk().value.flatMap(F.fromEither)

      _ <- if (shouldPerformRedownload) {
        cluster.compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
      } else F.unit

      _ <- logger.debug("Accepting all the checkpoint blocks received during the redownload.")
      _ <- acceptCheckpointBlocks().value.flatMap(F.fromEither)

    } yield ()

    cluster.getNodeState
      .map(NodeState.validForRedownload.contains)
      .ifM(
        wrappedCheck,
        logger.debug("Node state is not valid for redownload, skipping") >> F.unit
      )
  }

  private[redownload] def sendMajoritySnapshotsToCloud(): F[Unit] = {
    val send = for {
      majorityState <- lastMajorityState.get
      lastHeight <- lastSentHeight.get

      toSend = majorityState.filterKeys(_ > lastHeight)
      hashes = toSend.toList

      uploadSnapshots <- hashes.traverse {
        case (height, hash) =>
          snapshotStorage.getFile(hash).rethrowT.map {
            snapshotCloudStorage.write(height, hash, _).rethrowT
          }
      }

      uploadSnapshotInfos <- hashes.traverse {
        case (height, hash) =>
          snapshotInfoStorage.getFile(hash).rethrowT.map {
            snapshotInfoCloudStorage.write(height, hash, _).rethrowT
          }
      }

      // For now we do not restore EigenTrust model
      //      uploadRewards <- hashes.traverse {
      //        case (height, hash) =>
      //          rewardsStorage.getFile(hash).rethrowT.map {
      //            rewardsCloudStorage.write(height, hash, _).rethrowT
      //          }
      //      }

      _ <- uploadSnapshots.sequence
      _ <- uploadSnapshotInfos.sequence
      // For now we do not restore EigenTrust model
      //      _ <- uploadRewards.sequence

      _ <- if (toSend.nonEmpty) {
        val max = maxHeight(toSend)
        setLastSentHeight(max)
      } else F.unit

    } yield ()

    if (isEnabledCloudStorage) send.handleErrorWith(error => {
      logger.error(s"Sending snapshot to cloud failed with: ${error.getMessage}") >> metrics.incrementMetricAsync(
        "sendToCloud_failure"
      )
    })
    else F.unit
  }

  private[redownload] def rewardMajoritySnapshots(): EitherT[F, Throwable, Unit] =
    for {
      majorityState <- lastMajorityState.get.attemptT
      lastHeight <- rewardsManager.getLastRewardedHeight().attemptT

      _ <- EitherT.liftF(logger.debug(s"Last rewarded height is $lastHeight. Rewarding majority snapshots above."))

      majoritySubsetToReward = takeHighestUntilKey(majorityState, lastHeight)

      snapshotsToReward <- majoritySubsetToReward.toList.sortBy {
        case (height, _) => height
      }.traverse {
        case (height, hash) => snapshotStorage.read(hash).map(s => (height, s.snapshot))
      }

      _ <- snapshotsToReward.traverse {
        case (height, snapshot) => rewardsManager.attemptReward(snapshot, height)
      }.attemptT
    } yield ()

  private[redownload] def applyRedownloadPlan(
    plan: RedownloadPlan,
    majorityState: SnapshotsAtHeight
  ): EitherT[F, Throwable, Unit] =
    for {
      _ <- EitherT.liftF(logger.debug("Majority state:"))
      _ <- majorityState.toList.sortBy { case (height, _) => height }.traverse {
        case (height, hash) => logger.debug(s"[$height] - $hash")
      }.attemptT

      _ <- EitherT.liftF(logger.debug("To download:"))
      _ <- plan.toDownload.toList.sortBy { case (height, _) => height }.traverse {
        case (height, hash) => logger.debug(s"[$height] - $hash")
      }.attemptT

      _ <- EitherT.liftF(logger.debug("To remove:"))
      _ <- plan.toRemove.toList.sortBy { case (height, _) => height }.traverse {
        case (height, hash) => logger.debug(s"[$height] - $hash")
      }.attemptT

      _ <- EitherT.liftF(logger.debug("Fetching and storing missing snapshots on disk."))
      _ <- fetchAndStoreMissingSnapshots(plan.toDownload)

      _ <- EitherT.liftF(logger.debug("Filling missing createdSnapshots by majority state"))
      _ <- updateCreatedSnapshots(plan)

      _ <- EitherT.liftF(logger.debug("Replacing acceptedSnapshots by majority state"))
      _ <- updateAcceptedSnapshots(plan)

      _ <- EitherT.liftF(logger.debug("Fetching and persisting blocks above majority."))
      _ <- fetchAndPersistBlocksAboveMajority(majorityState)

      _ <- EitherT.liftF(logger.debug("RedownloadPlan has been applied succesfully."))

      _ <- EitherT.liftF(metrics.incrementMetricAsync("reDownloadFinished_total"))
    } yield ()

  private[redownload] def getIgnorePoint(maxHeight: Long): Long = maxHeight - meaningfulSnapshotsCount

  private[redownload] def getRemovalPoint(maxHeight: Long): Long = getIgnorePoint(maxHeight) - redownloadInterval * 2

  def removeUnacceptedSnapshotsFromDisk(): EitherT[F, Throwable, Unit] =
    for {
      nextSnapshotHash <- snapshotService.nextSnapshotHash.get.attemptT
      stored <- snapshotStorage.list().map(_.toSet)
      accepted <- getAcceptedSnapshots().attemptT.map(_.values.toSet)
      created <- getCreatedSnapshots().attemptT.map(_.values.map(_.hash).toSet)
      diff = stored.diff(accepted ++ created ++ Set(nextSnapshotHash))
      _ <- diff.toList.traverse { hash =>
        snapshotStorage.delete(hash) >> snapshotInfoStorage.delete(hash)
      }
    } yield ()

  private def takeHighestUntilKey[K <: Long, V](data: Map[K, V], key: K): Map[K, V] =
    data.filterKeys(_ > key)

  private[redownload] def updateCreatedSnapshots(plan: RedownloadPlan): EitherT[F, Throwable, Unit] =
    plan.toDownload.map {
      // IMPORTANT! persistCreatedSnapshot DOES NOT override existing values (!)
      case (height, hash) => persistCreatedSnapshot(height, hash, SortedMap.empty)
    }.toList.sequence.void.attemptT

  private[redownload] def updateAcceptedSnapshots(plan: RedownloadPlan): EitherT[F, Throwable, Unit] =
    acceptedSnapshots.modify { m =>
      // It removes everything from "ignored" pool!
      val updated = plan.toLeave ++ plan.toDownload // TODO: mwadon - is correct?
      // It leaves "ignored" pool untouched and aligns above "ignored" pool
      // val updated = (m -- plan.toRemove.keySet) |+| plan.toDownload
      (updated, ())
    }.attemptT

  private[redownload] def acceptCheckpointBlocks(): EitherT[F, Throwable, Unit] =
    (for {
      blocksToAccept <- snapshotService.syncBufferPull().map(_.values.toSeq.map(_.checkpointCacheData).distinct)
      _ <- checkpointAcceptanceService.waitingForAcceptance.modify { blocks =>
        val updated = blocks ++ blocksToAccept.map(_.checkpointBlock.soeHash)
        (updated, ())
      }
      _ <- TopologicalSort.sortBlocksTopologically(blocksToAccept).toList.traverse { b =>
        logger.debug(s"Accepting sync buffer block: ${b.height}") >>
          checkpointAcceptanceService.accept(b).handleErrorWith { error =>
            logger.warn(s"Error during buffer pool blocks acceptance after redownload: ${error.getMessage}") >> F.unit
          }
      }
    } yield ()).attemptT

  private[redownload] def updateHighestSnapshotInfo(): EitherT[F, Throwable, Unit] =
    for {
      highestSnapshotInfo <- getAcceptedSnapshots().attemptT
        .map(_.maxBy { case (height, _) => height } match { case (_, hash) => hash })
        .flatMap(hash => snapshotInfoStorage.read(hash))
      _ <- snapshotService.setSnapshot(highestSnapshotInfo).attemptT
    } yield ()

  private[redownload] def shouldRedownload(
    acceptedSnapshots: SnapshotsAtHeight,
    majorityState: SnapshotsAtHeight,
    redownloadInterval: Int,
    isDownload: Boolean = false
  ): Boolean =
    if (majorityState.isEmpty) false
    else
      isDownload || (getAlignmentResult(acceptedSnapshots, majorityState, redownloadInterval) match {
        case AlignedWithMajority => false
        case _                   => true
      })

  private[redownload] def calculateRedownloadPlan(
    acceptedSnapshots: SnapshotsAtHeight,
    majorityState: SnapshotsAtHeight
  ): RedownloadPlan = {
    val toDownload = majorityState.toSet.diff(acceptedSnapshots.toSet).toMap
    val toRemove = acceptedSnapshots.toSet.diff(majorityState.toSet).toMap
    val toLeave = acceptedSnapshots.toSet.diff(toRemove.toSet).toMap
    RedownloadPlan(toDownload, toRemove, toLeave)
  }

  private[redownload] def getAlignmentResult(
    acceptedSnapshots: SnapshotsAtHeight,
    majorityState: SnapshotsAtHeight,
    redownloadInterval: Int
  ): MajorityAlignmentResult = {
    val highestAcceptedHeight = maxHeight(acceptedSnapshots)
    val highestMajorityHeight = maxHeight(majorityState)

    val isAbove = highestAcceptedHeight > highestMajorityHeight
    val isBelow = highestAcceptedHeight < highestMajorityHeight
    val heightDiff = Math.abs(highestAcceptedHeight - highestMajorityHeight)
    val reachedRedownloadInterval = heightDiff >= redownloadInterval

    if (heightDiff == 0 && !majorityState.equals(acceptedSnapshots)) {
      MisalignedAtSameHeight
    } else if (isAbove && reachedRedownloadInterval) {
      AbovePastRedownloadInterval
    } else if (isAbove && !majorityState.toSet.subsetOf(acceptedSnapshots.toSet)) {
      AboveButMisalignedBeforeRedownloadIntervalAnyway
    } else if (isBelow && reachedRedownloadInterval) {
      BelowPastRedownloadInterval
    } else if (isBelow && !acceptedSnapshots.toSet.subsetOf(majorityState.toSet)) {
      BelowButMisalignedBeforeRedownloadIntervalAnyway
    } else {
      AlignedWithMajority
    }
  }

  private[redownload] def maxHeight[V](snapshots: Map[Long, V]): Long =
    if (snapshots.isEmpty) 0
    else snapshots.keySet.max

  private[redownload] def minHeight[V](snapshots: Map[Long, V]): Long =
    if (snapshots.isEmpty) 0
    else snapshots.keySet.min
}

object RedownloadService {

  def apply[F[_]: Concurrent: ContextShift: NonEmptyParallel: Timer](
    meaningfulSnapshotsCount: Int,
    redownloadInterval: Int,
    isEnabledCloudStorage: Boolean,
    cluster: Cluster[F],
    majorityStateChooser: MajorityStateChooser,
    snapshotStorage: LocalFileStorage[F, StoredSnapshot],
    snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
    rewardsStorage: LocalFileStorage[F, StoredRewards],
    snapshotService: SnapshotService[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F],
    snapshotCloudStorage: HeightHashFileStorage[F, StoredSnapshot],
    snapshotInfoCloudStorage: HeightHashFileStorage[F, SnapshotInfo],
    rewardsCloudStorage: HeightHashFileStorage[F, StoredRewards],
    rewardsManager: RewardsManager[F],
    apiClient: ClientInterpreter[F],
    metrics: Metrics
  ): RedownloadService[F] =
    new RedownloadService[F](
      meaningfulSnapshotsCount,
      redownloadInterval,
      isEnabledCloudStorage,
      cluster,
      majorityStateChooser,
      snapshotStorage,
      snapshotInfoStorage,
      rewardsStorage,
      snapshotService,
      checkpointAcceptanceService,
      snapshotCloudStorage,
      snapshotInfoCloudStorage,
      rewardsCloudStorage,
      rewardsManager,
      apiClient,
      metrics
    )

  type Reputation = SortedMap[Id, Double]
  type SnapshotsAtHeight = Map[Long, String] // height -> hash
  type SnapshotProposalsAtHeight = Map[Long, SnapshotProposal]
  type PeersProposals = Map[Id, SnapshotProposalsAtHeight]
  type PeersCache = Map[Id, NonEmptyList[MajorityHeight]]
  type SnapshotInfoSerialized = Array[Byte]
  type SnapshotSerialized = Array[Byte]

  implicit val snapshotProposalsAtHeightEncoder: Encoder[Map[Long, SnapshotProposal]] =
    Encoder.encodeMap[Long, SnapshotProposal]
  implicit val snapshotProposalsAtHeightDecoder: Decoder[Map[Long, SnapshotProposal]] =
    Decoder.decodeMap[Long, SnapshotProposal]

  case class LatestMajorityHeight(lowest: Long, highest: Long)

  object LatestMajorityHeight {
    implicit val latestMajorityHeightEncoder: Encoder[LatestMajorityHeight] = deriveEncoder
    implicit val latestMajorityHeightDecoder: Decoder[LatestMajorityHeight] = deriveDecoder
  }
}
