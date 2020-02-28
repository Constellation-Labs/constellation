package org.constellation.domain.redownload

import cats.implicits._
import org.constellation.domain.redownload.MajorityStateChooser.ExtendedSnapshotProposal
import org.constellation.domain.redownload.RedownloadService.{SnapshotProposalsAtHeight, SnapshotsAtHeight}
import org.constellation.schema.Id

import scala.collection.SortedMap

class MajorityStateChooser(id: Id) {

  // TODO: Use RedownloadService type definitions
  def chooseMajorityState(
    createdSnapshots: SnapshotProposalsAtHeight,
    peersProposals: Map[Id, SnapshotProposalsAtHeight]
  ): SnapshotsAtHeight = {
    val proposals = peersProposals + (id -> createdSnapshots)
    val proposersCount = proposals.size
    val mergedProposals = mergeByHeights(proposals, proposersCount)

    val flat = mergedProposals
      .mapValues(getTheMostQuantity(_, proposersCount))
      .mapValues(_.map(_.hash))

    for ((k, Some(v)) <- flat) yield k -> v
  }

  private def getTheMostQuantity(
    proposals: Set[ExtendedSnapshotProposal],
    proposersCount: Int
  ): Option[ExtendedSnapshotProposal] =
    proposals.toList.sortBy(o => (-o.trust, -o.percentage, o.hash)).headOption.flatMap { s =>
      if (areAllProposals(proposals, proposersCount) || isClearMajority(proposals)) {
        s.some
      } else None
    }

  private def areAllProposals(proposals: Set[ExtendedSnapshotProposal], proposersCount: Int): Boolean =
    proposals.toList.map(_.n).sum == proposersCount

  private def isClearMajority(proposals: Set[ExtendedSnapshotProposal]): Boolean =
    proposals.toList.count(_.totalPercentage >= 0.5) == 1

  private def mergeByHeights(
    proposals: Map[Id, SnapshotProposalsAtHeight],
    proposersCount: Int
  ): Map[Long, Set[ExtendedSnapshotProposal]] =
    proposals.map {
      case (id, v) =>
        v.mapValues(p => ExtendedSnapshotProposal(p.hash, p.reputation.getOrElse(id, 0.0), Set(id), 1, proposersCount))
    }.toList
      .map(_.mapValues(List(_)))
      .foldRight(Map.empty[Long, List[ExtendedSnapshotProposal]])(_ |+| _)
      .mapValues { heightProposals =>
        heightProposals
          .groupBy(_.hash)
          .map {
            case (hash, hashProposals) =>
              ExtendedSnapshotProposal(
                hash,
                hashProposals.foldRight(0.0)(_.trust + _),
                hashProposals.foldRight(Set.empty[Id])(_.ids ++ _),
                heightProposals.size,
                proposersCount
              )
          }
          .toSet
      }

}

object MajorityStateChooser {
  def apply(id: Id): MajorityStateChooser = new MajorityStateChooser(id)

  case class SnapshotProposal(hash: String, reputation: SortedMap[Id, Double])

  case class ExtendedSnapshotProposal(hash: String, trust: Double, ids: Set[Id], of: Int, proposersCount: Int) {
    val n: Int = ids.size

    val percentage: Double = n / of.toDouble
    val totalPercentage: Double = n / proposersCount.toDouble
  }
}
