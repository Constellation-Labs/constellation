package org.constellation.domain.redownload

import cats.implicits._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.constellation.domain.redownload.MajorityStateChooser.{ExtendedSnapshotProposal, SnapshotProposal}
import org.constellation.domain.redownload.RedownloadService.{
  PeersCache,
  PeersProposals,
  SnapshotProposalsAtHeight,
  SnapshotsAtHeight
}
import org.constellation.p2p.MajorityHeight
import org.constellation.schema.Id

import scala.collection.SortedMap

class MajorityStateChooser(id: Id) {

  // TODO: Use RedownloadService type definitions
  def chooseMajorityState(
    createdSnapshots: SnapshotProposalsAtHeight,
    peersProposals: PeersProposals,
    peersCache: PeersCache
  ): SnapshotsAtHeight = {
    val proposals = peersProposals + (id -> createdSnapshots)
    val filteredProposals = filterProposals(proposals, peersCache)
    val mergedProposals = mergeByHeights(filteredProposals, peersCache)

    val flat = mergedProposals
      .mapValues(getTheMostQuantity(_, peersCache))
      .mapValues(_.map(_.hash))

    flat.mapFilter(identity)
  }

  private def getTheMostQuantity(
    proposals: Set[ExtendedSnapshotProposal],
    peersCache: PeersCache
  ): Option[ExtendedSnapshotProposal] =
    proposals.toList.sortBy(o => (-o.trust, -o.percentage, o.hash)).headOption.flatMap { s =>
      if (areAllProposals(proposals, peersCache)) s.some
      else None
    }

  private def areAllProposals(proposals: Set[ExtendedSnapshotProposal], peersCache: PeersCache): Boolean =
    proposals.toList.map(_.n).sum == getProposersCount(proposals.headOption.map(_.height).getOrElse(-1L), peersCache)

  private def roundError(value: Double): Double =
    if (value < 1.0e-10 && value > -1.0e-10) 0d
    else value

  private def totalReputation(proposals: Map[Id, SnapshotProposalsAtHeight], height: Long, id: Id): Double =
    proposals.values.flatMap { _.get(height).map(_.reputation.getOrElse(id, 0d)) }.sum

  private def mergeByHeights(
    proposals: Map[Id, SnapshotProposalsAtHeight],
    peersCache: PeersCache
  ): Map[Long, Set[ExtendedSnapshotProposal]] =
    proposals.map {
      case (id, v) =>
        v.map {
          case (height, p) => {
            (
              height,
              ExtendedSnapshotProposal(
                p.hash,
                height,
                totalReputation(proposals, height, id),
                Set(id),
                1,
                getProposersCount(height, peersCache)
              )
            )
          }

        }
    }.toList
      .map(_.mapValues(List(_)))
      .foldRight(Map.empty[Long, List[ExtendedSnapshotProposal]])(_ |+| _)
      .mapValues { heightProposals =>
        heightProposals
          .groupBy(_.hash)
          .map {
            case (hash, hashProposals) => {
              val height = hashProposals.headOption.map(_.height).getOrElse(-1L)
              ExtendedSnapshotProposal(
                hash,
                height,
                roundError(hashProposals.foldRight(0.0)(_.trust + _)),
                hashProposals.foldRight(Set.empty[Id])(_.ids ++ _),
                heightProposals.size,
                getProposersCount(height, peersCache)
              )
            }
          }
          .toSet
      }

  private def getProposersCount(height: Long, peersCache: PeersCache): Int =
    peersCache.count {
      case (_, majorityHeight) => {
        MajorityHeight.isHeightBetween(height)(majorityHeight)
      }
    }

  private def filterProposals(
    peersProposals: PeersProposals,
    peersCache: PeersCache
  ): PeersProposals =
    peersProposals.filter {
      case (id, _) =>
        peersCache.contains(id)
    }.map {
      case (id, proposals) =>
        (id, proposals.filter {
          case (height, _) =>
            peersCache.get(id).exists(MajorityHeight.isHeightBetween(height))
        })
    }

}

object MajorityStateChooser {
  def apply(id: Id): MajorityStateChooser = new MajorityStateChooser(id)

  case class SnapshotProposal(hash: String, reputation: SortedMap[Id, Double])

  object SnapshotProposal {
    implicit val smDecoder: Decoder[SortedMap[Id, Double]] =
      Decoder.decodeMap[Id, Double].map(m => SortedMap(m.toSeq: _*))

    implicit val smEncoder: Encoder[SortedMap[Id, Double]] =
      Encoder.encodeMap[Id, Double].contramap(m => m.toMap)

    implicit val snapshotProposalEncoder: Encoder[SnapshotProposal] = deriveEncoder
    implicit val snapshotProposalDecoder: Decoder[SnapshotProposal] = deriveDecoder
  }

  case class ExtendedSnapshotProposal(
    hash: String,
    height: Long,
    trust: Double,
    ids: Set[Id],
    of: Int,
    proposersCount: Int
  ) {
    val n: Int = ids.size

    val percentage: Double = n / of.toDouble
    val totalPercentage: Double = n / proposersCount.toDouble
  }
}
