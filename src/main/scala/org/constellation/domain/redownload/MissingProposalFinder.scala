package org.constellation.domain.redownload

import cats.syntax.all._
import org.constellation.domain.redownload.RedownloadService.{PeersCache, PeersProposals, SnapshotsAtHeight}
import org.constellation.schema.Id
import org.constellation.schema.snapshot.HeightRange.MaxRange
import org.constellation.schema.snapshot.HeightRange

import scala.collection.immutable.NumericRange.inclusive
import scala.math.{max, min}

class MissingProposalFinder(
  heightInterval: Long,
  selfId: Id
) {

  /**
    * @return Map where the key is the Id of a peer and the value is a set of heights with missing proposals from the peer
    */
  def findMissingPeerProposals(
    lookupRange: HeightRange,
    proposals: PeersProposals,
    peersCache: PeersCache
  ): Set[(Id, Long)] = {
    val receivedProposals = proposals.mapValues(_.keySet)
    val bounds = lookupRange

    (peersCache - selfId)
      .mapValues(
        _.toList.mapFilter { majorityHeight =>
          majorityHeight.joined.map { joinedHeight =>
            val leftHeight = majorityHeight.left.getOrElse(MaxRange.to)
            HeightRange(joinedHeight, leftHeight)
          }
        }
      )
      .toList
      .flatMap {
        case (id, majorityHeights) =>
          val allProposals = expandIntervals(bounds, majorityHeights)
          (allProposals -- receivedProposals.getOrElse(id, Set.empty)).map((id, _)).toList
      }
      .toSet
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
}

object MissingProposalFinder {

  def apply(heightInterval: Long, selfId: Id) =
    new MissingProposalFinder(heightInterval, selfId)
}
