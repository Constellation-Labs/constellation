package org.constellation.infrastructure.redownload

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.chrisdavenport.mapref.MapRef
import org.constellation.concurrency.MapRefUtils
import org.constellation.concurrency.MapRefUtils.MapRefOps
import org.constellation.domain.redownload.RedownloadService.{
  PeersProposals,
  ProposalCoordinate,
  Reputation,
  SnapshotProposalsAtHeight,
  SnapshotsAtHeight
}
import org.constellation.domain.redownload.{MissingProposalFinder, RedownloadPlan, RedownloadStorageAlgebra}
import org.constellation.schema.Id
import org.constellation.schema.signature.Signed
import org.constellation.schema.signature.Signed.signed
import org.constellation.schema.snapshot.{FilterData, HeightRange, SnapshotProposal}
import java.security.KeyPair

import org.constellation.collection.MapUtils._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.concurrency.cuckoo.{CuckooFilter, MutableCuckooFilter}

import scala.collection.immutable.SortedMap

class RedownloadStorageInterpreter[F[_]](
  keyPair: KeyPair
)(implicit F: Sync[F])
    extends RedownloadStorageAlgebra[F] {

  private val logger = Slf4jLogger.getLogger[F]

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
  private val peerProposals: MapRef[F, Id, Option[SnapshotProposalsAtHeight]] =
    MapRefUtils.ofConcurrentHashMap()

  private val lastMajorityState: Ref[F, SnapshotsAtHeight] = Ref.unsafe(Map.empty)
  private val lastSentHeight: Ref[F, Long] = Ref.unsafe(-1L)

  private val majorityStallCount: Ref[F, Int] = Ref.unsafe(0)

  private val localFilter: MutableCuckooFilter[F, ProposalCoordinate] =
    MutableCuckooFilter[F, ProposalCoordinate]()

  private val remoteFilters: MapRef[F, Id, Option[CuckooFilter]] =
    MapRefUtils.ofConcurrentHashMap()

  def getCreatedSnapshots: F[SnapshotProposalsAtHeight] = createdSnapshots.get

  def persistCreatedSnapshot(height: Long, hash: String, reputation: Reputation): F[Unit] =
    createdSnapshots.modify { m =>
      val updated =
        if (m.contains(height)) m
        else m.updated(height, signed(SnapshotProposal(hash, height, reputation), keyPair))
      (updated, updated.maxHeight)
    }

  def takeHighestUntilKey[K <: Long, V](data: Map[K, V], key: K): Map[K, V] =
    data.filterKeys(_ > key)

  def maxHeight[V](snapshots: Map[Long, V]): Long =
    if (snapshots.isEmpty) 0
    else snapshots.keySet.max

  def getAcceptedSnapshots: F[SnapshotsAtHeight] = acceptedSnapshots.get

  def persistAcceptedSnapshot(height: Long, hash: String): F[Unit] =
    acceptedSnapshots.modify { m =>
      val updated = m.updated(height, hash)
      (updated, updated.maxHeight)
    }

  def replacePeerProposals(peer: Id, proposals: SnapshotProposalsAtHeight): F[Unit] =
    peerProposals(peer).get.flatMap { maybeMap =>
      maybeMap.traverse(removeFromLocalFilter)
    } >> peerProposals(peer).set(proposals.some) >> addToLocalFilter(proposals)

  private def removeFromLocalFilter(proposals: SnapshotProposalsAtHeight): F[Unit] =
    proposals.values.toList.map { spp =>
      (spp.signature.id, spp.value.height)
    }.traverse { p =>
      localFilter
        .delete(p)
        .ifM(F.unit, logger.warn(s"Proposal $p could not be removed from local filter"))
    } >> F.unit

  def persistPeerProposal(proposal: Signed[SnapshotProposal]): F[Unit] =
    logger.debug(
      s"Persisting proposal of ${proposal.signature.id.hex} at height ${proposal.value.height} and hash ${proposal.value.hash}"
    ) >> persistPeerProposals(List(proposal))

  def persistPeerProposals(proposals: Iterable[Signed[SnapshotProposal]]): F[Unit] =
    proposals
      .groupBy(_.signature.id)
      .toList
      .traverse {
        case (id, proposals) =>
          for {
            addedProposals <- peerProposals(id)
              .modify[SnapshotProposalsAtHeight] { maybeMap =>
                val oldProposals = maybeMap.getOrElse(Map.empty)
                val newProposals = proposals.map(s => (s.value.height, s)).toMap
                ((oldProposals ++ newProposals).some, newProposals -- oldProposals.keySet)
              }
            _ <- addToLocalFilter(addedProposals)
          } yield (addedProposals)
      }
      .void

  private def addToLocalFilter(proposals: SnapshotProposalsAtHeight): F[Unit] =
    proposals.values.toList.map { spp =>
      (spp.signature.id, spp.value.height)
    }.traverse { p =>
      localFilter
        .insert(p)
        .ifM(F.unit, F.raiseError(new RuntimeException(s"Unable to insert proposal $p to local filter")))
    } >> F.unit

  def getPeersProposals: F[Map[Id, SnapshotProposalsAtHeight]] = peerProposals.toMap

  def getPeerProposals(peer: Id): F[Option[SnapshotProposalsAtHeight]] = peerProposals(peer).get

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

  def clear(): F[Unit] =
    for {
      _ <- createdSnapshots.modify(_ => (Map.empty, ()))
      _ <- acceptedSnapshots.modify(_ => (Map.empty, ()))
      _ <- peerProposals.clear
      _ <- localFilter.clear
      _ <- remoteFilters.clear
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

  def removeSnapshotsAndProposalsBelowHeight(
    height: Long
  ): F[(SnapshotsAtHeight, SnapshotProposalsAtHeight, PeersProposals)] =
    for {
      as <- acceptedSnapshots.updateAndGet(_.removeHeightsBelow(height))
      cs <- createdSnapshots.updateAndGet(_.removeHeightsBelow(height))
      pp <- removePeerProposalsBelowHeight(height)
    } yield (as, cs, pp)

  private def removePeerProposalsBelowHeight(height: Long): F[PeersProposals] =
    for {
      ids <- peerProposals.keys
      results <- ids.traverse { id =>
        peerProposals(id).modify { maybeMap =>
          val oldProposals = maybeMap.getOrElse(Map.empty)
          val removedProposals = oldProposals.filterKeys(_ < height)
          val result = oldProposals.removeHeightsBelow(height)
          (result.some, (removedProposals, result))
        }.flatMap { case (removed, result) => removeFromLocalFilter(removed) >> F.pure((id, result)) }
      }
    } yield results.toMap

  def replaceRemoteFilterData(peerId: Id, filterData: FilterData): F[Unit] =
    remoteFilters(peerId).set(CuckooFilter(filterData).some)

  def getRemoteFilters: F[Map[Id, CuckooFilter]] = remoteFilters.toMap

  def localFilterData: F[FilterData] = localFilter.getFilterData

  def getMajorityStallCount: F[Int] = majorityStallCount.get

  def resetMajorityStallCount: F[Unit] = majorityStallCount.set(0)

  def incrementMajorityStallCount: F[Unit] = majorityStallCount.update(_ + 1)

  implicit val proposalCoordinateToString: ProposalCoordinate => String = {
    case (id, height) => s"$id:$height"
  }
}
