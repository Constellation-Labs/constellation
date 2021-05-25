package org.constellation.infrastructure.redownload

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.chrisdavenport.mapref.MapRef
import org.constellation.concurrency.MapRefUtils
import org.constellation.concurrency.MapRefUtils.MapRefOps
import org.constellation.domain.redownload.RedownloadService.{Reputation, SnapshotProposalsAtHeight, SnapshotsAtHeight}
import org.constellation.domain.redownload.{MissingProposalFinder, RedownloadPlan, RedownloadStorageAlgebra}
import org.constellation.schema.Id
import org.constellation.schema.signature.Signed
import org.constellation.schema.signature.Signed.signed
import org.constellation.schema.snapshot.{HeightRange, MajorityInfo, SnapshotProposal}

import java.security.KeyPair
import scala.collection.immutable.SortedMap

class RedownloadStorageInterpreter[F[_]](
  missingProposalFinder: MissingProposalFinder,
  meaningfulSnapshotsCount: Int,
  redownloadInterval: Int,
  keyPair: KeyPair
)(implicit F: Sync[F])
    extends RedownloadStorageAlgebra[F] {

  /**
    * It contains immutable historical data
    * (even incorrect snapshots which have been "fixed" by majority in acceptedSnapshots).
    * It is used as own proposals along with peerProposals to calculate majority state.
    */
  private val createdSnapshots: Ref[F, SnapshotProposalsAtHeight] = Ref.unsafe(Map.empty)

  /**
    * Can be modified ("fixed"/"aligned") by redownload process. It stores current state of snapshots after auto-healing.
    */
  private val acceptedSnapshots: Ref[F, SnapshotsAtHeight] = Ref.unsafe(Map.empty)

  /**
    * Majority proposals from other peers. It is used to calculate majority state.
    */
  private val peersProposals: MapRef[F, Id, Option[SnapshotProposalsAtHeight]] =
    MapRefUtils.ofConcurrentHashMap()

  private val lastMajorityState: Ref[F, SnapshotsAtHeight] = Ref.unsafe(Map.empty)
  private val peerMajorityInfo: MapRef[F, Id, Option[MajorityInfo]] = MapRefUtils.ofConcurrentHashMap()
  private val lastSentHeight: Ref[F, Long] = Ref.unsafe(-1L)

  def getCreatedSnapshots: F[SnapshotProposalsAtHeight] = createdSnapshots.get

  def persistCreatedSnapshot(height: Long, hash: String, reputation: Reputation): F[Unit] =
    createdSnapshots.modify { m =>
      val updated =
        if (m.contains(height)) m
        else {
          m.updated(height, signed(SnapshotProposal(hash, height, reputation), keyPair))
        }
      val max = maxHeight(updated)
      val removalPoint = getRemovalPoint(max)
      val limited = takeHighestUntilKey(updated, removalPoint)
      (limited, ())
    }

  def getRemovalPoint(maxHeight: Long): Long = getIgnorePoint(maxHeight) - redownloadInterval * 2

  def getIgnorePoint(maxHeight: Long): Long = maxHeight - meaningfulSnapshotsCount

  def takeHighestUntilKey[K <: Long, V](data: Map[K, V], key: K): Map[K, V] =
    data.filterKeys(_ > key)

  def maxHeight[V](snapshots: Map[Long, V]): Long =
    if (snapshots.isEmpty) 0
    else snapshots.keySet.max

  def getAcceptedSnapshots: F[SnapshotsAtHeight] = acceptedSnapshots.get

  def persistAcceptedSnapshot(height: Long, hash: String): F[Unit] =
    acceptedSnapshots.modify { m =>
      val updated = m.updated(height, hash)
      val max = maxHeight(updated)
      val removalPoint = getRemovalPoint(max)
      val limited = takeHighestUntilKey(updated, removalPoint)
      (limited, ())
    }

  def getPeersProposals: F[Map[Id, SnapshotProposalsAtHeight]] = peersProposals.toMap

  def getPeerProposals(peer: Id): F[Option[SnapshotProposalsAtHeight]] = peersProposals(peer).get

  def replacePeerProposals(peer: Id, proposals: SnapshotProposalsAtHeight): F[Unit] =
    peersProposals(peer).set(proposals.some)

  def persistPeerProposal(peer: Id, proposal: Signed[SnapshotProposal]): F[Unit] =
    persistPeerProposals(peer, Map(proposal.value.height -> proposal))

  def persistPeerProposals(peer: Id, proposals: SnapshotProposalsAtHeight): F[Unit] =
    peersProposals(peer).modify { maybeMap =>
      val updatedMap = maybeMap.getOrElse(Map.empty) ++ proposals
      val trimmedMap = takeHighestUntilKey(updatedMap, getRemovalPoint(maxHeight(updatedMap)))
      (trimmedMap.some, ())
    }

  def getLastMajorityState: F[SnapshotsAtHeight] =
    lastMajorityState.get

  def getLastSentHeight: F[Long] = lastSentHeight.get

  def getLatestMajorityHeight: F[Long] = lastMajorityState.modify { s =>
    (s, maxHeight(s))
  }

  def getLowestMajorityHeight: F[Long] = lastMajorityState.modify { s =>
    (s, minHeight(s))
  }

  def getMajorityRange: F[HeightRange] = lastMajorityState.get.map { s =>
    HeightRange(minHeight(s), maxHeight(s))
  }

  def minHeight[V](snapshots: Map[Long, V]): Long =
    if (snapshots.isEmpty) 0
    else snapshots.keySet.min

  def getMajorityGapRanges: F[List[HeightRange]] = lastMajorityState.get.map(missingProposalFinder.findGapRanges)

  def getPeerMajorityInfo: F[Map[Id, MajorityInfo]] = peerMajorityInfo.toMap

  def updatePeerMajorityInfo(peerId: Id, majorityInfo: MajorityInfo): F[Unit] =
    peerMajorityInfo(peerId).set(majorityInfo.some)

  def clear(): F[Unit] =
    for {
      _ <- createdSnapshots.modify(_ => (Map.empty, ()))
      _ <- acceptedSnapshots.modify(_ => (Map.empty, ()))
      _ <- peersProposals.clear
      _ <- setLastMajorityState(Map.empty)
      _ <- setLastSentHeight(-1)
    } yield ()

  def setLastMajorityState(majorityState: SnapshotsAtHeight): F[Unit] =
    lastMajorityState.modify { _ =>
      (majorityState, maxHeight(majorityState))
    }

  def setLastSentHeight(height: Long): F[Unit] =
    lastSentHeight.modify { _ =>
      (height, ())
    }

  def updateCreatedSnapshots(plan: RedownloadPlan): F[Unit] =
    plan.toDownload.map {
      // IMPORTANT! persistCreatedSnapshot DOES NOT override existing values (!)
      case (height, hash) => persistCreatedSnapshot(height, hash, SortedMap.empty)
    }.toList.sequence.void

  def updateAcceptedSnapshots(plan: RedownloadPlan): F[Unit] =
    acceptedSnapshots.modify { m =>
      // It removes everything from "ignored" pool!
      val updated = plan.toLeave ++ plan.toDownload
      // It leaves "ignored" pool untouched and aligns above "ignored" pool
      // val updated = (m -- plan.toRemove.keySet) |+| plan.toDownload
      (updated, ())
    }
}
