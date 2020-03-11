package org.constellation.domain.redownload

import cats.NonEmptyParallel
import cats.data.EitherT
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.CheckpointAcceptanceService
import org.constellation.consensus.{FinishedCheckpoint, StoredSnapshot}
import org.constellation.domain.cloud.CloudStorage
import org.constellation.domain.redownload.MajorityStateChooser.SnapshotProposal
import org.constellation.domain.redownload.RedownloadService._
import org.constellation.domain.snapshot.{SnapshotInfo, SnapshotInfoStorage}
import org.constellation.domain.storage.FileStorage
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.NodeState
import org.constellation.rewards.RewardsManager
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.storage.SnapshotService
import org.constellation.util.{APIClient, Metrics}

import scala.collection.SortedMap
import scala.concurrent.duration._
import scala.util.Random

class RedownloadService[F[_]: NonEmptyParallel](
  meaningfulSnapshotsCount: Int,
  redownloadInterval: Int,
  isEnabledCloudStorage: Boolean,
  cluster: Cluster[F],
  majorityStateChooser: MajorityStateChooser,
  snapshotStorage: FileStorage[F, StoredSnapshot],
  snapshotInfoStorage: SnapshotInfoStorage[F],
  snapshotService: SnapshotService[F],
  checkpointAcceptanceService: CheckpointAcceptanceService[F],
  cloudStorage: CloudStorage[F],
  rewardsManager: RewardsManager[F],
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

  private[redownload] var lastRewardedHeight: Ref[F, Long] = Ref.unsafe(-1L)

  private val logger = Slf4jLogger.getLogger[F]

  def persistCreatedSnapshot(height: Long, hash: String, reputation: Reputation): F[Unit] =
    createdSnapshots.modify { m =>
      val updated = if (m.contains(height)) m else m.updated(height, SnapshotProposal(hash, reputation))
      val max = maxHeight(updated)
      val removalPoint = getRemovalPoint(max)
      val limited = takeHighestUntilKey(updated, removalPoint)
      (limited, ())
    }

  def persistAcceptedSnapshot(height: Long, hash: String): F[Unit] =
    acceptedSnapshots.modify { m =>
      val updated = m.updated(height, hash)
      val max = maxHeight(updated)
      val removalPoint = getRemovalPoint(max)
      val limited = takeHighestUntilKey(updated, removalPoint)
      (limited, ())
    }

  def getCreatedSnapshots(): F[SnapshotProposalsAtHeight] = createdSnapshots.get

  def getAcceptedSnapshots(): F[SnapshotsAtHeight] = acceptedSnapshots.get

  def getPeerProposals(): F[PeersProposals] = peersProposals.get

  def fetchAndUpdatePeersProposals(): F[PeersProposals] =
    for {
      _ <- logger.debug("Fetching and updating peer proposals")
      peers <- cluster.getPeerInfo.map(_.values.toList)
      apiClients = peers.map(_.client)
      responses <- apiClients.traverse { client =>
        fetchCreatedSnapshots(client).map(client.id -> _)
      }
      proposals <- peersProposals.modify { m =>
        val updated = m ++ responses.toMap
        val ignored = updated.mapValues(a => takeHighestUntilKey(a, getRemovalPoint(maxHeight(a))))
        (ignored, ignored)
      }
    } yield proposals

  private[redownload] def fetchStoredSnapshotsFromAllPeers(): F[Map[APIClient, Set[String]]] =
    for {
      peers <- cluster.getPeerInfo.map(_.values.toList)
      apiClients = peers.map(_.client)
      responses <- apiClients.traverse { client =>
        fetchStoredSnapshots(client)
          .map(client -> _.toSet)
      }
    } yield responses.toMap

  // TODO: Extract to HTTP layer
  private def fetchAcceptedSnapshots(client: APIClient): F[SnapshotsAtHeight] =
    client
      .getNonBlockingF[F, SnapshotsAtHeight]("snapshot/accepted")(C)
      .handleErrorWith(_ => F.pure(Map.empty))

  // TODO: Extract to HTTP layer
  private def fetchCreatedSnapshots(client: APIClient): F[SnapshotProposalsAtHeight] =
    client
      .getNonBlockingF[F, SnapshotProposalsAtHeight]("snapshot/created")(C)
      .handleErrorWith { e =>
        logger.error(s"Fetch peers proposals error: ${e.getMessage}") >> F.pure(Map.empty)
      }

  // TODO: Extract to HTTP layer
  private def fetchStoredSnapshots(client: APIClient): F[Seq[String]] =
    client
      .getNonBlockingF[F, Seq[String]]("snapshot/stored")(C)
      .handleErrorWith(_ => F.pure(Seq.empty))

  // TODO: Extract to HTTP layer
  private def fetchSnapshotInfo(client: APIClient): F[SnapshotInfoSerialized] =
    client
      .getNonBlockingArrayByteF("snapshot/info", timeout = 45.second)(C)

  // TODO: Extract to HTTP layer
  private def fetchSnapshotInfo(hash: String)(client: APIClient): F[SnapshotInfoSerialized] =
    client
      .getNonBlockingArrayByteF("snapshot/info/" + hash, timeout = 45.second)(C)

  // TODO: Extract to HTTP layer
  private def fetchSnapshot(hash: String)(client: APIClient): F[SnapshotSerialized] =
    client
      .getNonBlockingArrayByteF("storedSnapshot/" + hash, timeout = 45.second)(C)

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
          snapshotStorage.write(hash, snapshot) >> snapshotInfoStorage.writeSnapshotInfo(hash, snapshotInfo)
      }.void
    } yield ()

  private def useRandomClient[A](pool: Set[APIClient], attempts: Int = 3)(fetchFn: APIClient => F[A]): F[A] =
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

      majoritySnapshotInfo <- snapshotInfoStorage.readSnapshotInfo(maxMajorityHash)

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

          blocksFromSnapshots = acceptedSnapshots.flatMap(_.checkpointCache)
          acceptedBlocksFromSnapshotInfo = snapshotInfoFromMemPool.acceptedCBSinceSnapshotCache
          awaitingBlocksFromSnapshotInfo = snapshotInfoFromMemPool.awaitingCbs
          blocksToAccept = (blocksFromSnapshots ++ acceptedBlocksFromSnapshotInfo ++ awaitingBlocksFromSnapshotInfo)
            .sortBy(_.height)

          _ <- snapshotService.setSnapshot(majoritySnapshotInfo)

          _ <- blocksToAccept.traverse { b =>
            logger.debug(s"Accepting block above majority: ${b.height}") >> checkpointAcceptanceService
              .accept(b)
              .handleErrorWith(
                error => logger.warn(s"Error during blocks acceptance after redownload: ${error.getMessage}") >> F.unit
              )
          }
        } yield ()
      }.attemptT
    } yield ()

  def checkForAlignmentWithMajoritySnapshot(): F[Unit] = {
    val wrappedCheck = for {
      _ <- logger.debug("Checking alignment with majority snapshot...")
      peersProposals <- getPeerProposals()
      createdSnapshots <- getCreatedSnapshots()
      acceptedSnapshots <- getAcceptedSnapshots()

      majorityState = majorityStateChooser.chooseMajorityState(
        createdSnapshots,
        peersProposals
      )

      maxMajorityHeight = maxHeight(majorityState)
      ignorePoint = getIgnorePoint(maxMajorityHeight)
      meaningfulAcceptedSnapshots = takeHighestUntilKey(acceptedSnapshots, ignorePoint)
      meaningfulMajorityState = takeHighestUntilKey(majorityState, ignorePoint)

      _ <- lastMajorityState.modify(_ => (meaningfulMajorityState, ()))

      _ <- if (shouldRedownload(meaningfulAcceptedSnapshots, meaningfulMajorityState, redownloadInterval)) {
        val plan = calculateRedownloadPlan(meaningfulAcceptedSnapshots, meaningfulMajorityState)
        val result = getAlignmentResult(meaningfulAcceptedSnapshots, meaningfulMajorityState, redownloadInterval)
        for {
          _ <- logger.debug(s"Alignment result: $result")
          _ <- logger.debug("Redownload needed! Applying the following redownload plan:")
          _ <- applyRedownloadPlan(plan, meaningfulMajorityState).value.flatMap(F.fromEither)
        } yield ()
      } else logger.debug("No redownload needed - snapshots have been already aligned with majority state. ")

      _ <- logger.debug("Sending majority snapshots to cloud (if enabled).")
      _ <- sendMajoritySnapshotsToCloud()

      _ <- logger.debug("Rewarding majority snapshots.")
      _ <- rewardMajoritySnapshots().value.flatMap(F.fromEither)

      _ <- logger.debug("Removing unaccepted snapshots from disk.")
      _ <- removeUnacceptedSnapshotsFromDisk().value.flatMap(F.fromEither)

      _ <- cluster.compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
    } yield ()

    cluster.compareAndSet(NodeState.validForRedownload, NodeState.DownloadInProgress).flatMap { state =>
      if (state.isNewSet) {
        wrappedCheck.handleErrorWith { error =>
          logger.error(s"Redownload error: ${error.getMessage}") >> cluster
            .compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
            .void
        }
      } else F.unit
    }
  }

  private[redownload] def sendMajoritySnapshotsToCloud(): F[Unit] = {
    val send = for {
      majorityState <- lastMajorityState.get
      lastHeight <- lastSentHeight.get

      toSend = majorityState.filterKeys(_ > lastHeight)
      hashes = toSend.values.toList
      snapshotFiles <- snapshotStorage.getFiles(hashes).rethrowT
      snapshotInfoFiles <- snapshotInfoStorage.getSnapshotInfoFiles(hashes)

      _ <- cloudStorage.upload(snapshotFiles, "snapshots".some)
      _ <- cloudStorage.upload(snapshotInfoFiles, "snapshot-infos".some)
      _ <- if (toSend.nonEmpty) {
        val max = maxHeight(toSend)
        lastSentHeight.modify(_ => (max, ()))
      } else F.unit

    } yield ()

    if (isEnabledCloudStorage) send else F.unit
  }

  private[redownload] def rewardMajoritySnapshots(): EitherT[F, Throwable, Unit] =
    for {
      majorityState <- lastMajorityState.get.attemptT
      lastHeight <- lastRewardedHeight.get.attemptT

      _ <- EitherT.liftF(logger.debug(s"Last rewarded height is $lastHeight. Rewarding majority snapshots above."))

      majoritySubsetToReward = takeHighestUntilKey(majorityState, lastHeight)

      snapshotsToReward <- majoritySubsetToReward.toList.sortBy {
        case (height, _) => height
      }.traverse {
        case (height, hash) => snapshotStorage.read(hash).map(s => (height, s.snapshot))
      }

      _ <- snapshotsToReward.traverse {
        case (height, snapshot) =>
          rewardsManager.attemptReward(snapshot, height) >> lastRewardedHeight.modify(_ => (height, ()))
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

      _ <- EitherT.liftF(logger.debug("Accepting all the checkpoint blocks received during the redownload."))
      _ <- acceptCheckpointBlocks()

      _ <- EitherT.liftF(logger.debug("RedownloadPlan has been applied succesfully."))

      _ <- EitherT.liftF(metrics.incrementMetricAsync("reDownloadFinished_total"))
    } yield ()

  private[redownload] def getIgnorePoint(maxHeight: Long): Long = maxHeight - meaningfulSnapshotsCount

  private[redownload] def getRemovalPoint(maxHeight: Long): Long = getIgnorePoint(maxHeight) - redownloadInterval * 2

  def removeUnacceptedSnapshotsFromDisk(): EitherT[F, Throwable, Unit] =
    for {
      stored <- snapshotStorage.list().map(_.toSet)
      accepted <- getAcceptedSnapshots().attemptT.map(_.values.toSet)
      created <- getCreatedSnapshots().attemptT.map(_.values.map(_.hash).toSet)
      diff = stored.diff(accepted ++ created)
      _ <- diff.toList.traverse { hash =>
        snapshotStorage.delete(hash) >> snapshotInfoStorage.removeSnapshotInfo(hash)
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

  private[redownload] def acceptCheckpointBlocks()(
    implicit ord: Ordering[FinishedCheckpoint]
  ): EitherT[F, Throwable, Unit] =
    snapshotService
      .syncBufferPull()
      .flatMap(
        _.values.toList
          .sortBy(_.checkpointCacheData.height)
          .traverse(
            b =>
              logger
                .debug(s"Accepting sync buffer block: ${b.checkpointCacheData.height}") >> checkpointAcceptanceService
                .accept(b)
                .handleErrorWith(
                  error =>
                    logger.warn(s"Error during blocks acceptance after redownload: ${error.getMessage}") >> F.unit
                )
          )
      )
      .void
      .attemptT

  private[redownload] def updateHighestSnapshotInfo(): EitherT[F, Throwable, Unit] =
    for {
      highestSnapshotInfo <- getAcceptedSnapshots().attemptT
        .map(_.maxBy { case (height, _) => height } match { case (_, hash) => hash })
        .flatMap(hash => snapshotInfoStorage.readSnapshotInfo(hash))
      _ <- snapshotService.setSnapshot(highestSnapshotInfo).attemptT
    } yield ()

  private[redownload] def shouldRedownload(
    acceptedSnapshots: SnapshotsAtHeight,
    majorityState: SnapshotsAtHeight,
    redownloadInterval: Int
  ): Boolean =
    getAlignmentResult(acceptedSnapshots, majorityState, redownloadInterval) match {
      case AlignedWithMajority => false
      case _                   => true
    }

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
}

object RedownloadService {

  def apply[F[_]: Concurrent: ContextShift: NonEmptyParallel: Timer](
    meaningfulSnapshotsCount: Int,
    redownloadInterval: Int,
    isEnabledCloudStorage: Boolean,
    cluster: Cluster[F],
    majorityStateChooser: MajorityStateChooser,
    snapshotStorage: FileStorage[F, StoredSnapshot],
    snapshotInfoStorage: SnapshotInfoStorage[F],
    snapshotService: SnapshotService[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F],
    cloudStorage: CloudStorage[F],
    rewardsManager: RewardsManager[F],
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
      snapshotService,
      checkpointAcceptanceService,
      cloudStorage,
      rewardsManager,
      metrics
    )

  type Reputation = SortedMap[Id, Double]
  type SnapshotsAtHeight = Map[Long, String] // height -> hash
  type SnapshotProposalsAtHeight = Map[Long, SnapshotProposal]
  type PeersProposals = Map[Id, SnapshotProposalsAtHeight]
  type SnapshotInfoSerialized = Array[Byte]
  type SnapshotSerialized = Array[Byte]
}
