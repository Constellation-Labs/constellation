package org.constellation.domain.redownload

import cats.data.EitherT
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil
import org.constellation.checkpoint.CheckpointAcceptanceService
import org.constellation.domain.cloud.CloudStorage
import org.constellation.domain.redownload.RedownloadService.{
  PeersProposals,
  SnapshotInfoSerialized,
  SnapshotSerialized,
  SnapshotsAtHeight
}
import org.constellation.domain.snapshot.{SnapshotInfoStorage, SnapshotStorage}
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.NodeState
import org.constellation.schema.Id
import org.constellation.storage.SnapshotService
import org.constellation.util.{APIClient, Metrics}

import scala.concurrent.duration._
import scala.util.Random

class RedownloadService[F[_]](
  meaningfulSnapshotsCount: Int,
  redownloadInterval: Int,
  isEnabledCloudStorage: Boolean,
  cluster: Cluster[F],
  majorityStateChooser: MajorityStateChooser,
  snapshotStorage: SnapshotStorage[F],
  snapshotInfoStorage: SnapshotInfoStorage[F],
  snapshotService: SnapshotService[F],
  checkpointAcceptanceService: CheckpointAcceptanceService[F],
  cloudStorage: CloudStorage[F],
  metrics: Metrics
)(implicit F: Concurrent[F], C: ContextShift[F]) {

  /**
    * It contains immutable historical data
    * (even incorrect snapshots which have been "fixed" by majority in acceptedSnapshots).
    * It is used as own proposals along with peerProposals to calculate majority state.
    */
  private[redownload] val createdSnapshots: Ref[F, SnapshotsAtHeight] = Ref.unsafe(Map.empty)

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

  def persistCreatedSnapshot(height: Long, hash: String): F[Unit] =
    createdSnapshots.modify { m =>
      val updated = if (m.contains(height)) m else m.updated(height, hash)
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

  def getCreatedSnapshots(): F[SnapshotsAtHeight] = createdSnapshots.get

  def getAcceptedSnapshots(): F[SnapshotsAtHeight] = acceptedSnapshots.get

  def getPeerProposals(): F[PeersProposals] = peersProposals.get

  def getCreatedSnapshot(height: Long): F[Option[String]] =
    getCreatedSnapshots().map(_.get(height))

  def fetchAndUpdatePeersProposals(): F[PeersProposals] =
    for {
      _ <- logger.debug("Fetching and updating peer proposals")
      peers <- cluster.getPeerInfo.map(_.values.toList)
      apiClients = peers.map(_.client)
      responses <- apiClients.traverse { client =>
        client
          .getNonBlockingF[F, SnapshotsAtHeight]("snapshot/own")(C)
          .handleErrorWith { e =>
            logger.error(s"Fetch peers proposals error: ${e.getMessage}") >> F.pure(Map.empty)
          }
          .map(client.id -> _)
      }
      proposals <- peersProposals.modify { m =>
        val updated = m ++ responses.toMap
        (updated, updated)
      }
    } yield proposals

  private[redownload] def fetchStoredSnapshotsFromAllPeers(): F[Map[APIClient, Seq[String]]] =
    for {
      peers <- cluster.getPeerInfo.map(_.values.toList)
      apiClients = peers.map(_.client)
      responses <- apiClients.traverse { client =>
        fetchStoredSnapshots(client)
          .map(client -> _)
      }
    } yield responses.toMap

  private[redownload] def fetchSnapshotAndSnapshotInfoFromRandomPeer(
    hash: String,
    pool: Iterable[APIClient]
  ): F[(SnapshotSerialized, SnapshotInfoSerialized)] = {
    val poolArray = pool.toArray
    val stopAt = Random.nextInt(poolArray.length)

    def makeAttempt(index: Int): F[(SnapshotSerialized, SnapshotInfoSerialized)] =
      (for {
        snapshot <- fetchSnapshot(hash, poolArray(index))
        snapshotInfo <- fetchSnapshotInfo(hash, poolArray(index))
      } yield (snapshot, snapshotInfo)).handleErrorWith {
        case e if index == stopAt => F.raiseError[(SnapshotSerialized, SnapshotInfoSerialized)](e)
        case _                    => makeAttempt((index + 1) % poolArray.length)
      }

    makeAttempt((stopAt + 1) % poolArray.length)
  }

  private def takeHighestUntilKey[K <: Long, V](data: Map[K, V], key: K): Map[K, V] =
    data.filterKeys(_ > key)

  private def fetchStoredSnapshots(client: APIClient): F[Seq[String]] =
    client
      .getNonBlockingF[F, Seq[String]]("snapshot/stored")(C)
      .handleErrorWith(_ => F.pure(Seq.empty))

  private def fetchSnapshotInfo(hash: String, client: APIClient): F[SnapshotInfoSerialized] =
    client
      .getNonBlockingArrayByteF("snapshot/info/" + hash, timeout = 45.second)(C)

  private def fetchSnapshot(hash: String, client: APIClient): F[SnapshotSerialized] =
    client
      .getNonBlockingArrayByteF("storedSnapshot/" + hash, timeout = 45.second)(C)

  private def fetchAndStoreMissingSnapshots(snapshotsToDownload: SnapshotsAtHeight): EitherT[F, Throwable, Unit] =
    for {
      storedSnapshots <- fetchStoredSnapshotsFromAllPeers().attemptT
      candidates = snapshotsToDownload.values.toList.map { hash =>
        (hash, storedSnapshots.filter { case (_, hashes) => hashes.contains(hash) }.keySet)
      }.toMap
      missingSnapshots <- candidates.toList.traverse {
        case (hash, pool) => fetchSnapshotAndSnapshotInfoFromRandomPeer(hash, pool).map(snapshot => (hash, snapshot))
      }.map(_.toMap).attemptT

      _ <- missingSnapshots.toList.traverse {
        case (hash, (snapshot, snapshotInfo)) =>
          snapshotStorage.writeSnapshot(hash, snapshot) >> snapshotInfoStorage.writeSnapshotInfo(hash, snapshotInfo)
      }.void
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

      _ <- lastMajorityState.modify(_ => (majorityState, ()))

      _ <- if (shouldRedownload(meaningfulAcceptedSnapshots, meaningfulMajorityState, redownloadInterval)) {
        val plan = calculateRedownloadPlan(meaningfulAcceptedSnapshots, meaningfulMajorityState)
        val result = getAlignmentResult(meaningfulAcceptedSnapshots, meaningfulMajorityState, redownloadInterval)
        for {
          _ <- logger.debug(s"Alignment result: $result")
          _ <- logger.debug("Redownload needed! Applying the following redownload plan:")
          _ <- applyRedownloadPlan(plan).value.flatMap(F.fromEither)
        } yield ()
      } else logger.debug("No redownload needed - snapshots have been already aligned with majority state. ")

      _ <- logger.debug("Sending majority snapshots to cloud (if enabled).")
      _ <- sendMajoritySnapshotsToCloud()

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
      snapshotFiles <- snapshotStorage.getSnapshotFiles(hashes)
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

  private[redownload] def applyRedownloadPlan(plan: RedownloadPlan): EitherT[F, Throwable, Unit] =
    for {
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

      _ <- EitherT.liftF(logger.debug("Replacing acceptedSnapshots by majority state"))
      _ <- updateAcceptedSnapshots(plan)

      _ <- EitherT.liftF(logger.debug("Updating highest SnapshotInfo in SnapshotService."))
      _ <- updateHighestSnapshotInfo()

      _ <- EitherT.liftF(logger.debug("Accepting all the checkpoint blocks received during the redownload."))
      _ <- acceptCheckpointBlocks()

      _ <- EitherT.liftF(logger.debug("RedownloadPlan has been applied succesfully."))

      _ <- EitherT.liftF(metrics.incrementMetricAsync("dag_reDownloadFinished_total"))
    } yield ()

  private[redownload] def getIgnorePoint(maxHeight: Long): Long = maxHeight - meaningfulSnapshotsCount

  private[redownload] def getRemovalPoint(maxHeight: Long): Long = getIgnorePoint(maxHeight) - redownloadInterval * 2

  def removeUnacceptedSnapshotsFromDisk(): EitherT[F, Throwable, Unit] =
    for {
      stored <- snapshotStorage.getSnapshotHashes.attemptT.map(_.toSet)
      accepted <- getAcceptedSnapshots().attemptT.map(_.values.toSet)
      created <- getCreatedSnapshots().attemptT.map(_.values.toSet)
      diff = stored.diff(accepted ++ created)
      _ <- diff.toList.traverse { hash =>
        snapshotStorage.removeSnapshot(hash) >> snapshotInfoStorage.removeSnapshotInfo(hash)
      }
    } yield ()

  private[redownload] def updateAcceptedSnapshots(plan: RedownloadPlan): EitherT[F, Throwable, Unit] =
    acceptedSnapshots.modify { m =>
      // It removes everything from "ignored" pool!
      val updated = plan.toLeave |+| plan.toDownload
      // It leaves "ignored" pool untouched and aligns above "ignored" pool
      // val updated = (m -- plan.toRemove.keySet) |+| plan.toDownload
      (updated, ())
    }.attemptT

  private[redownload] def acceptCheckpointBlocks(): EitherT[F, Throwable, Unit] =
    snapshotService
      .syncBufferPull()
      .flatMap(_.values.toList.traverse(checkpointAcceptanceService.accept(_)))
      .attemptT
      .void

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

  private[redownload] def maxHeight(snapshots: SnapshotsAtHeight): Long =
    if (snapshots.isEmpty) 0
    else snapshots.keySet.max
}

object RedownloadService {

  def apply[F[_]: Concurrent: ContextShift](
    meaningfulSnapshotsCount: Int,
    redownloadInterval: Int,
    isEnabledCloudStorage: Boolean,
    cluster: Cluster[F],
    majorityStateChooser: MajorityStateChooser,
    snapshotStorage: SnapshotStorage[F],
    snapshotInfoStorage: SnapshotInfoStorage[F],
    snapshotService: SnapshotService[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F],
    cloudStorage: CloudStorage[F],
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
      metrics
    )

  type SnapshotsAtHeight = Map[Long, String] // height -> hash
  type PeersProposals = Map[Id, SnapshotsAtHeight]
  type SnapshotInfoSerialized = Array[Byte]
  type SnapshotSerialized = Array[Byte]
}
