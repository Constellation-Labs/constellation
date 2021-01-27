package org.constellation.domain.redownload

import cats.data.NonEmptyList
import cats.syntax.all._
import org.constellation.invertedmap.InvertedMap
import org.constellation.p2p.MajorityHeight
import org.constellation.schema.Id
import org.constellation.schema.signature.{HashSignature, Signed}
import org.constellation.schema.snapshot.{HeightRange, MajorityInfo, SnapshotProposal}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.OptionValues._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedMap

class MissingProposalFinderTest
    extends AnyFreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {

  private val finder = MissingProposalFinder(2, 0, 10, Id("self"))

  private val toPeerProposals: ((Id, List[Int])) => (Id, Map[Long, Signed[SnapshotProposal]]) = {
    case (id, heights) =>
      (id, heights.map { h =>
        (h.toLong, Signed(HashSignature("s", id), SnapshotProposal(s"${id.hex}_$h", 0L, SortedMap.empty)))
      }.toMap)
  }

  private def joinedAt(height: Long) = MajorityHeight(height.some, none[Long])

  private def leftAt(height: Long) = MajorityHeight(none[Long], height.some)

  private def joinedAndLeftAt(joinedHeight: Long, leftHeight: Long) = MajorityHeight(joinedHeight.some, leftHeight.some)

  private def nel[T](head: T, tail: T*) = NonEmptyList(head, tail.toList)

  "findGaps and findGapRanges" - {
    "should find single gaps" in {
      val majority = Map[Long, String](
        2L -> "a",
        4L -> "b",
        6L -> "c",
        10L -> "d",
        12L -> "e",
        16L -> "f"
      )

      val gaps = finder.findGaps(majority)
      gaps should contain only (8L, 14L)

      val gapRanges = finder.findGapRanges(majority)
      gapRanges should contain only (HeightRange(8, 8), HeightRange(14, 14))
    }

    "should find multiple gaps" in {
      val majority = Map[Long, String](
        2L -> "a",
        4L -> "b",
        12L -> "e",
        16L -> "f"
      )

      val gaps = finder.findGaps(majority)
      gaps should contain only (6L, 8L, 10L, 14L)

      val gapRanges = finder.findGapRanges(majority)
      gapRanges should contain only (HeightRange(6, 10), HeightRange(14, 14))
    }

    "should not find any gaps for empty majority" in {
      val majority = Map.empty[Long, String]

      val gaps = finder.findGaps(majority)
      gaps shouldBe empty

      val gapRanges = finder.findGapRanges(majority)
      gapRanges shouldBe empty
    }

    "should not find any gaps gaps for single item majority" in {
      val majority = Map(
        2L -> "a"
      )
      val gaps = finder.findGaps(majority)
      gaps shouldBe empty

      val gapRanges = finder.findGapRanges(majority)
      gapRanges shouldBe empty
    }
  }

  "findMissingPeerProposals" - {
    "should find missing proposals" in {
      val peerProposals = InvertedMap(
        Id("a") -> List(2, 4, 6),
        Id("b") -> List(2, 4),
        Id("c") -> List(4)
      ).map(toPeerProposals)
      val peersCache = Map(
        Id("a") -> nel(joinedAt(2)),
        Id("b") -> nel(joinedAt(2)),
        Id("c") -> nel(joinedAt(2))
      )
      val majorityRange = HeightRange(2, 6)

      val result = finder.findMissingPeerProposals(majorityRange, peerProposals, peersCache)

      result should (contain.key(Id("b")).and(contain).key(Id("c")))
      result.get(Id("b")).value should contain only (6L)
      result.get(Id("c")).value should contain only (2L, 6L)
    }

    "should not find missing proposals when nodes were absent" in {
      val peerProposals = InvertedMap(
        Id("a") -> List(2, 4),
        Id("b") -> List(2, 4),
        Id("c") -> List(2, 6)
      ).map(toPeerProposals)
      val peersCache = Map(
        Id("a") -> nel(joinedAndLeftAt(2, 4)),
        Id("b") -> nel(leftAt(4)),
        Id("c") -> nel(leftAt(2), joinedAt(6))
      )
      val majorityRange = HeightRange(2, 6)

      val result = finder.findMissingPeerProposals(majorityRange, peerProposals, peersCache)
      result shouldBe empty
    }
  }

  "selectPeerForFetchingMissingProposals" - {
    "should select peer that has the least gaps" in {
      val peerMajorityInfo = Map(
        Id("a") -> MajorityInfo(HeightRange(2, 6), List(HeightRange(4, 6))),
        Id("b") -> MajorityInfo(HeightRange(2, 6), List(HeightRange(4, 4))),
        Id("c") -> MajorityInfo(HeightRange(2, 6), List(HeightRange(2, 4)))
      )
      val missingProposals = Set(2L, 4L, 6L)
      val majorityRange = HeightRange(2, 6)

      val result = finder.selectPeerForFetchingMissingProposals(majorityRange, missingProposals, peerMajorityInfo)
      result should contain(Id("b"))
    }

    "should not select any peer" in {
      val peerMajorityInfo = Map(
        Id("a") -> MajorityInfo(HeightRange(2, 10), List(HeightRange(6, 6))),
        Id("b") -> MajorityInfo(HeightRange(8, 10), List.empty),
        Id("c") -> MajorityInfo(HeightRange(2, 4), List.empty)
      )

      val missingProposals = Set(6L)

      val bounds = HeightRange(2, 6)

      val result = finder.selectPeerForFetchingMissingProposals(bounds, missingProposals, peerMajorityInfo)
      result shouldBe empty
    }
  }

}
