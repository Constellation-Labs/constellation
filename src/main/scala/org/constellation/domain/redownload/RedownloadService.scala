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

  def checkForAlignmentWithMajoritySnapshot(): F[Unit] =
    for {
      peersProposals <- getPeerProposals()
      createdSnapshots <- getCreatedSnapshots()
      majorityState = majorityStateChooser.chooseMajorityState(
        createdSnapshots,
        peersProposals
      )
      // TODO: Calculate diff and call redownload if true
      _ <- if (shouldRedownload(createdSnapshots, majorityState, snapshotHeightRedownloadDelayInterval)) F.unit
      else F.unit
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

  private[redownload] def getAlignmentResult(
    acceptedSnapshots: SnapshotsAtHeight,
    majorityState: SnapshotsAtHeight,
    redownloadInterval: Int
  ): MajorityAlignmentResult = {
    val highestCreatedHeight = acceptedSnapshots.maxBy { case (height, _) => height } match {
      case (height, _) => height
    }
    val highestCreatedHeightFromMajority = majorityState.maxBy { case (height, _) => height } match {
      case (height, _) => height
    }

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
