package org.constellation.domain.redownload

import cats.data.EitherT
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil
import org.constellation.checkpoint.CheckpointAcceptanceService
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
import org.constellation.util.APIClient

import scala.concurrent.duration._
import scala.util.Random

class RedownloadService[F[_]](
  cluster: Cluster[F],
  majorityStateChooser: MajorityStateChooser,
  snapshotStorage: SnapshotStorage[F],
  snapshotInfoStorage: SnapshotInfoStorage[F],
  snapshotService: SnapshotService[F],
  checkpointAcceptanceService: CheckpointAcceptanceService[F]
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

  private[redownload] val snapshotHeightRedownloadDelayInterval: Int =
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval")

  private val logger = Slf4jLogger.getLogger[F]

  def persistCreatedSnapshot(height: Long, hash: String): F[Unit] =
    createdSnapshots.modify { m =>
      val updated = if (m.contains(height)) m else m.updated(height, hash)
      (updated, ())
    }

  def persistAcceptedSnapshot(height: Long, hash: String): F[Unit] =
    acceptedSnapshots.modify { m =>
      val updated = m.updated(height, hash)
      (updated, ())
    }

  def getCreatedSnapshots(): F[SnapshotsAtHeight] = createdSnapshots.get

  def getAcceptedSnapshots(): F[SnapshotsAtHeight] = acceptedSnapshots.get

  def getPeerProposals(): F[PeersProposals] = peersProposals.get

  def getCreatedSnapshot(height: Long): F[Option[String]] =
    getCreatedSnapshots().map(_.get(height))

  def fetchAndUpdatePeersProposals: F[PeersProposals] =
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

  private[redownload] def fetchStoredSnapshotsFromAllPeers: F[Map[APIClient, Seq[String]]] =
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

  private def fetchAndStoreMissingSnapshots(snapshotsToDownload: SnapshotsAtHeight): F[Unit] =
    for {
      storedSnapshots <- fetchStoredSnapshotsFromAllPeers
      candidates = snapshotsToDownload.values.toList.map { hash =>
        (hash, storedSnapshots.filter { case (_, hashes) => hashes.contains(hash) }.keySet)
      }.toMap
      missingSnapshots <- candidates.toList.traverse {
        case (hash, pool) => fetchSnapshotAndSnapshotInfoFromRandomPeer(hash, pool).map(snapshot => (hash, snapshot))
      }.map(_.toMap)
      _ <- missingSnapshots.toList.traverse {
        case (hash, (snapshot, snapshotInfo)) =>
          snapshotStorage.writeSnapshot(hash, snapshot) >> snapshotInfoStorage.writeSnapshotInfo(hash, snapshotInfo)
      }.value.void
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

      _ <- if (shouldRedownload(acceptedSnapshots, majorityState, snapshotHeightRedownloadDelayInterval)) {
        val plan = calculateRedownloadPlan(acceptedSnapshots, majorityState)
        for {
          _ <- logger.debug("Redownload needed! - applying the following redownload plan:")
          _ <- logger.debug("To download:")
          _ <- plan.toDownload.toList.sortBy { case (height, _) => height }.traverse {
            case (height, hash) => logger.debug(s"[$height] - $hash")
          }
          _ <- logger.debug("To remove:")
          _ <- plan.toRemove.toList.sortBy { case (height, _) => height }.traverse {
            case (height, hash) => logger.debug(s"[$height] - $hash")
          }
          _ <- applyRedownloadPlan(plan).value
        } yield ()
      } else logger.debug("No redownload needed - snapshots have been already aligned with majority state. ")
      _ <- cluster.compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
    } yield ()

    cluster.compareAndSet(NodeState.validForRedownload, NodeState.DownloadInProgress).flatMap { state =>
      if (state.isNewSet) {
        wrappedCheck.handleErrorWith { _ =>
          cluster.compareAndSet(NodeState.validDuringDownload, NodeState.Ready).void
        }
      } else F.unit
    }
  }

  private[redownload] def applyRedownloadPlan(plan: RedownloadPlan): EitherT[F, Throwable, Unit] =
    for {
      _ <- EitherT.liftF(logger.debug("Fetching missing snapshots."))
      _ <- fetchAndStoreMissingSnapshots(plan.toDownload).attemptT
      _ <- EitherT.liftF(logger.debug("Updating acceptedSnapshots by majority state."))
      _ <- acceptedSnapshots.modify { m =>
        val updated = plan.toLeave |+| plan.toDownload
        (updated, updated)
      }.attemptT

      _ <- EitherT.liftF(logger.debug("Checking which SnapshotInfo is the highest one."))

      highestSnapshotInfo <- getAcceptedSnapshots().attemptT
        .map(_.maxBy { case (height, _) => height } match { case (_, hash) => hash })
        .flatMap(hash => snapshotInfoStorage.readSnapshotInfo(hash))

      _ <- EitherT.liftF(logger.debug("Updating highest SnapshotInfo in SnapshotService."))
      _ <- snapshotService.setSnapshot(highestSnapshotInfo).attemptT

      _ <- EitherT.liftF(logger.debug("Accepting all the checkpoint blocks from the highest SnapshotInfo."))
      _ <- snapshotService
        .syncBufferPull()
        .flatMap(_.values.toList.traverse(checkpointAcceptanceService.accept(_)))
        .attemptT

      _ <- EitherT.liftF(logger.debug("Redownload plan has been applied succesfully."))

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
    val highestCreatedHeight = maxHeight(acceptedSnapshots)
    val highestCreatedHeightFromMajority = maxHeight(majorityState)

    val isAbove = highestCreatedHeight > highestCreatedHeightFromMajority
    val isBelow = highestCreatedHeight < highestCreatedHeightFromMajority
    val heightDiff = Math.abs(highestCreatedHeight - highestCreatedHeightFromMajority)
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
    else
      snapshots.maxBy { case (height, _) => height } match {
        case (height, _) => height
      }
}

object RedownloadService {

  def apply[F[_]: Concurrent: ContextShift](
    cluster: Cluster[F],
    majorityStateChooser: MajorityStateChooser,
    snapshotStorage: SnapshotStorage[F],
    snapshotInfoStorage: SnapshotInfoStorage[F],
    snapshotService: SnapshotService[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F]
  ): RedownloadService[F] =
    new RedownloadService[F](
      cluster,
      majorityStateChooser,
      snapshotStorage,
      snapshotInfoStorage,
      snapshotService,
      checkpointAcceptanceService
    )

  type SnapshotsAtHeight = Map[Long, String] // height -> hash
  type PeersProposals = Map[Id, SnapshotsAtHeight]
  type SnapshotInfoSerialized = Array[Byte]
  type SnapshotSerialized = Array[Byte]
}
