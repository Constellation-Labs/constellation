package org.constellation.domain.redownload

import cats.implicits._
import org.constellation.domain.redownload.MajorityStateChooser.{ExtendedSnapshotProposal, SnapshotProposal}
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
      if (areAllProposals(proposals, proposersCount)) s.some
      else None
    }

  private def areAllProposals(proposals: Set[ExtendedSnapshotProposal], proposersCount: Int): Boolean =
    proposals.toList.map(_.n).sum == proposersCount

  private def roundError(value: Double): Double =
    if (value < 1.0e-10 && value > -1.0e-10) 0d
    else value

  private def totalReputation(proposals: Map[Id, SnapshotProposalsAtHeight], height: Long, id: Id): Double =
    proposals.values.flatMap { _.get(height).map(_.reputation.getOrElse(id, 0d)) }.sum

  private def mergeByHeights(
    proposals: Map[Id, SnapshotProposalsAtHeight],
    proposersCount: Int
  ): Map[Long, Set[ExtendedSnapshotProposal]] =
    proposals.map {
      case (id, v) =>
        v.map {
          case (height, p) =>
            (
              height,
              ExtendedSnapshotProposal(p.hash, totalReputation(proposals, height, id), Set(id), 1, proposersCount)
            )
        }
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
                roundError(hashProposals.foldRight(0.0)(_.trust + _)),
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
