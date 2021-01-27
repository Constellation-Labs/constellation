package org.constellation.domain.redownload

import cats.syntax.all._
import org.constellation.domain.redownload.RedownloadService.{PeersCache, PeersProposals, SnapshotsAtHeight}
import org.constellation.schema.Id
import org.constellation.schema.snapshot.HeightRange.MaxRange
import org.constellation.schema.snapshot.{HeightRange, MajorityInfo}

import scala.collection.immutable.NumericRange.inclusive
import scala.math.{max, min}

class MissingProposalFinder(
  private val heightInterval: Long,
  private val offset: Long,
  private val limit: Long,
  private val selfId: Id
) {

  /**
    * @return Map where the key is the Id of a peer and the value is a set of heights with missing proposals from the peer
    */
  def findMissingPeerProposals(
    majorityRange: HeightRange,
    proposals: PeersProposals,
    peersCache: PeersCache
  ): Map[Id, Set[Long]] = {
    val receivedProposals = proposals.mapValues(_.keySet)
    val bounds = boundedRange(majorityRange)

    (peersCache - selfId)
      .mapValues(
        _.map { majorityHeight =>
          HeightRange(majorityHeight.joined.getOrElse(MaxRange.from), majorityHeight.left.getOrElse(MaxRange.to))
        }.toList
      )
      .map {
        case (id, majorityHeights) =>
          val allProposals = expandIntervals(bounds, majorityHeights)
          (id, allProposals -- receivedProposals.getOrElse(id, Set.empty))
      }
      .filter {
        case (_, missingProposals) => missingProposals.nonEmpty
      }
  }

  /**
    * @return Id of the peer that can provide the most proposals that are missing, if one could be found
    */
  def selectPeerForFetchingMissingProposals(
    majorityRange: HeightRange,
    missingProposals: Set[Long],
    peerMajorityInfo: Map[Id, MajorityInfo]
  ): Option[Id] = {
    val bounds = boundedRange(majorityRange)

    (peerMajorityInfo - selfId).mapValues { majorityInfo =>
      val peerGaps = expandIntervals(bounds, majorityInfo.majorityGaps) ++
        rangeSet(majorityInfo.majorityRange.to + heightInterval, bounds.to) ++
        rangeSet(bounds.from, majorityInfo.majorityRange.from - heightInterval)
      missingProposals -- peerGaps
    }.toList.filter { case (_, proposalsToFill) => proposalsToFill.nonEmpty }.maximumByOption {
      case (_, proposalsToFill) => proposalsToFill.size
    }.map { case (id, _) => id }
  }

  def findGapRanges(majorityState: SnapshotsAtHeight): List[HeightRange] =
    majorityState.keySet.toList.sorted
      .sliding(2)
      .toList
      .mapFilter {
        case List(prev, curr) =>
          if (curr - prev != heightInterval)
            HeightRange(prev + heightInterval, curr - heightInterval).some
          else
            none[HeightRange]
        case List(_) =>
          none[HeightRange]
      }

  def findGaps(majorityState: SnapshotsAtHeight): Set[Long] =
    expandIntervals(MaxRange, findGapRanges(majorityState))

  private def expandIntervals(bounds: HeightRange, intervals: List[HeightRange]): Set[Long] =
    intervals.foldMap { interval =>
      val from = max(interval.from, bounds.from)
      val to = min(interval.to, bounds.to)
      rangeSet(from, to)
    }

  private def rangeSet(from: Long, to: Long): Set[Long] =
    inclusive(from, to, heightInterval).toSet

  private def boundedRange(range: HeightRange) =
    HeightRange(max(range.from, range.to - (offset + limit)), range.to - offset)

}

object MissingProposalFinder {

  def apply(heightInterval: Long, offset: Long, limit: Long, selfId: Id) =
    new MissingProposalFinder(heightInterval, offset, limit, selfId)
}
