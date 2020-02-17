package org.constellation.domain.redownload

import cats.effect.{Concurrent, ContextShift}
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.ConfigUtil
import org.constellation.domain.redownload.RedownloadService.{PeersProposals, SnapshotsAtHeight}
import org.constellation.p2p.Cluster
import org.constellation.schema.Id

class RedownloadService[F[_]](
  cluster: Cluster[F],
  majorityStateChooser: MajorityStateChooser
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
      peers <- cluster.getPeerInfo.map(_.values.toList)
      apiClients = peers.map(_.client)
      responses <- apiClients.traverse { client =>
        client
          .getNonBlockingF[F, SnapshotsAtHeight]("/snapshot/own")(C)
          .handleErrorWith(_ => F.pure(Map.empty))
          .map(client.id -> _)
      }
      proposals <- peersProposals.modify { m =>
        val updated = m ++ responses.toMap
        (updated, updated)
      }
    } yield proposals

  def fetchMissingSnapshots: F[SnapshotsAtHeight] = ??? // TODO: Implement

  def checkForAlignmentWithMajoritySnapshot(): F[Unit] =
    for {
      peersProposals <- getPeerProposals()
      createdSnapshots <- getCreatedSnapshots()
      acceptedSnapshots <- getAcceptedSnapshots()

      majorityState = majorityStateChooser.chooseMajorityState(
        createdSnapshots,
        peersProposals
      )

      _ <- if (shouldRedownload(acceptedSnapshots, majorityState, snapshotHeightRedownloadDelayInterval)) {
        val plan = calculateRedownloadPlan(acceptedSnapshots, majorityState)
        applyRedownloadPlan(plan) // TODO: Actual redownload, rethink splitting the logic
      } else F.unit
    } yield ()

  private[redownload] def applyRedownloadPlan(plan: RedownloadPlan): F[Unit] = for {
    missingSnapshots <- fetchMissingSnapshots // TODO: Store these snapshots on disk now?
    _ <- acceptedSnapshots.modify { m =>
      val updated = plan.expectedResult // TODO: Is it the correct way?
      (updated, ())
    }
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
    val toDownload = (majorityState.toSet diff acceptedSnapshots.toSet).toMap
    val toRemove = (acceptedSnapshots.toSet diff majorityState.toSet).toMap
    val toLeave = (acceptedSnapshots.toSet diff toRemove.toSet).toMap
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
    else snapshots.maxBy { case (height, _) => height } match { case (height, _) => height }
}

object RedownloadService {

  def apply[F[_]: Concurrent: ContextShift](
    cluster: Cluster[F],
    majorityStateChooser: MajorityStateChooser
  ): RedownloadService[F] =
    new RedownloadService[F](cluster, majorityStateChooser)

  type SnapshotsAtHeight = Map[Long, String] // height -> hash
  type PeersProposals = Map[Id, SnapshotsAtHeight]
}
