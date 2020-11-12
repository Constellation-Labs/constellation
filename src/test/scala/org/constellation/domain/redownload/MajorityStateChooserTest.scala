package org.constellation.domain.redownload

import cats.data.NonEmptyList
import cats.syntax.all._
import org.constellation.domain.redownload.MajorityStateChooser.SnapshotProposal
import org.constellation.domain.redownload.RedownloadService.{
  PeersCache,
  PeersProposals,
  SnapshotProposalsAtHeight,
  SnapshotsAtHeight
}
import org.constellation.invertedmap.InvertedMap
import org.constellation.p2p.MajorityHeight
import org.constellation.schema.v2.Id
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedMap

class MajorityStateChooserTest
    extends AnyFreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {

  val ownId = Id("z")

  implicit class MapImpl(value: Map[Long, String]) {
    val toSnapshotProposals = value.mapValues(SnapshotProposal(_, SortedMap.empty))
  }

  implicit class NMapImpl(value: Map[Id, Map[Long, String]]) {
    val toSnapshotProposals = InvertedMap(value.mapValues(_.mapValues(SnapshotProposal(_, SortedMap.empty))))
  }

  "find gaps" - {
    "finds single gaps" in {
      val majority = Map[Long, String](
        2L -> "a",
        4L -> "b",
        6L -> "c",
        10L -> "d",
        12L -> "e",
        16L -> "f"
      )

      val chooser = MajorityStateChooser(ownId)
      val gaps = chooser.findGaps(majority, 2)
      gaps shouldEqual Set(8L, 14L)
    }

    "finds multiple gaps" in {
      val majority = Map[Long, String](
        2L -> "a",
        4L -> "b",
        12L -> "e",
        16L -> "f"
      )

      val chooser = MajorityStateChooser(ownId)
      val gaps = chooser.findGaps(majority, 2)
      gaps shouldEqual Set(6L, 8L, 10L, 14L)
    }

    "returns np gaps for empty majority" in {
      val majority = Map.empty[Long, String]
      val chooser = MajorityStateChooser(ownId)
      val gaps = chooser.findGaps(majority, 2)
      gaps shouldEqual Set.empty[Long]
    }

    "returns np gaps for single item majority" in {
      val majority = Map(
        2L -> "a"
      )
      val chooser = MajorityStateChooser(ownId)
      val gaps = chooser.findGaps(majority, 2)
      gaps shouldEqual Set.empty[Long]
    }
  }

  "choose majority state" - {
    "returns Map.empty" - {
      "if there are no created snapshots and peers proposals" - {
        chooseMajorityState(Map.empty, InvertedMap.empty, Map.empty) shouldBe Map.empty
      }

      "if there are no created snapshots and there are 2 peers (but only one with snapshot)" in {
        val peersCache =
          List("z", "a", "b").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
        val createdSnapshots = Map.empty[Long, SnapshotProposal]
        val peersProposals = Map(
          Id("a") -> Map(2L -> "aa"),
          Id("b") -> Map.empty[Long, String]
        ).toSnapshotProposals

        chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe Map.empty
      }

      "if there is one created snapshot and there are 2 peers without snapshots" in {
        val peersCache =
          List("z", "a", "b").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
        val createdSnapshots = Map(2L -> "aa").toSnapshotProposals
        val peersProposals = InvertedMap(
          Id("a") -> Map.empty[Long, SnapshotProposal],
          Id("b") -> Map.empty[Long, SnapshotProposal]
        )

        chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe Map.empty
      }

      "if all nodes joined higher than proposals" in {
        val peersCache =
          List("z", "a", "b").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight(Some(10L), None))).toMap
        val createdSnapshots = Map(2L -> "aa").toSnapshotProposals
        val peersProposals = Map(
          Id("a") -> Map(2L -> "aa"),
          Id("b") -> Map(2L -> "aa")
        ).toSnapshotProposals

        chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe Map.empty
      }
    }

    "if there is only one height" - {
      "and not all peers made snapshot" - {
        "returns empty Map without majority snapshot at this height" in {
          val peersCache =
            List("z", "a", "b", "c").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
          val createdSnapshots = Map(2L -> "aa").toSnapshotProposals
          val peersProposals = Map(
            Id("a") -> Map(2L -> "bb"),
            Id("b") -> Map(2L -> "bb"),
            Id("c") -> Map.empty[Long, String]
          ).toSnapshotProposals
          val result = Map.empty[Long, String]

          chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
        }
      }
    }

    "if there are more than one heights" - {
      "returns majority snapshot at each height if there is a clear majority (more than 50%)" in {
        val peersCache =
          List("z", "a", "b").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
        val createdSnapshots = Map(2L -> "aa", 4L -> "bb", 6L -> "cc").toSnapshotProposals
        val peersProposals = Map(
          Id("a") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd"),
          Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "ccc", 8L -> "ddd")
        ).toSnapshotProposals
        val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

        chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
      }
    }

    "if there is 50%-50% split" - {
      "if there is no reputation" - {
        "returns the first hash in alphabetical order" in {
          val peersCache =
            List("z", "a", "b", "c").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
          val createdSnapshots = Map(2L -> "xx", 4L -> "yy", 6L -> "zz").toSnapshotProposals
          val peersProposals = Map(
            Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz"),
            Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc"),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc")
          ).toSnapshotProposals
          val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

          chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
        }

        "does not return consistent continuation but treats each height individually" in {
          val peersCache =
            List("z", "a", "b", "c").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
          val createdSnapshots = Map(2L -> "xx", 4L -> "dd", 6L -> "zz").toSnapshotProposals
          val peersProposals = Map(
            Id("a") -> Map(2L -> "xx", 4L -> "dd", 6L -> "zz"),
            Id("b") -> Map(2L -> "aa", 4L -> "ee", 6L -> "cc"),
            Id("c") -> Map(2L -> "aa", 4L -> "ee", 6L -> "cc")
          ).toSnapshotProposals
          val result = Map(2L -> "aa", 4L -> "dd", 6L -> "cc")

          chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
        }
      }

      "if there is reputation" - {
        "return the majority calculated by trust proposers" in {
          val peersCache =
            List("z", "a", "b", "c").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
          val trust = SortedMap[Id, Double](Id("z") -> 1, Id("a") -> 1, Id("b") -> 0.1, Id("c") -> 0.1)
          val createdSnapshots = Map(2L -> "xx", 4L -> "yy", 6L -> "zz").mapValues(SnapshotProposal(_, trust))
          val peersProposals = InvertedMap(
            Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz").mapValues(SnapshotProposal(_, trust)),
            Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trust)),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trust))
          )

          val result = Map(2L -> "xx", 4L -> "yy", 6L -> "zz")

          chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
        }
      }

      "if there is positive and negative reputation" - {
        "return the majority calculated by trust proposers" in {
          val peersCache =
            List("z", "a", "b", "c").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
          val trust = SortedMap[Id, Double](Id("z") -> 1, Id("a") -> -1, Id("b") -> -0.1, Id("c") -> -0.1)
          val createdSnapshots = Map(2L -> "xx", 4L -> "yy", 6L -> "zz").mapValues(SnapshotProposal(_, trust))
          val peersProposals = InvertedMap(
            Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz").mapValues(SnapshotProposal(_, trust)),
            Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trust)),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trust))
          )

          val result = Map(2L -> "xx", 4L -> "yy", 6L -> "zz")

          chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
        }
      }
    }

    "if there is no clear majority (less than 50%)" - {
      "and each proposal has the same quantity" - {
        "returns the first hash in alphabetical order" in {
          val peersCache =
            List("z", "a", "b").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
          val createdSnapshots = Map(2L -> "aa", 4L -> "ee", 6L -> "ii").toSnapshotProposals
          val peersProposals = Map(
            Id("a") -> Map(2L -> "dd", 4L -> "bb", 6L -> "ff"),
            Id("b") -> Map(2L -> "gg", 4L -> "hh", 6L -> "cc")
          ).toSnapshotProposals
          val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

          chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
        }
      }

      "and at least one proposal has bigger quantity" - {
        "returns the proposal with the most quantity" in {
          val peersCache =
            List("z", "a", "b", "c", "d", "e", "f", "g", "h")
              .map(Id(_))
              .map(_ -> NonEmptyList.one(MajorityHeight.genesis))
              .toMap
          val createdSnapshots = Map(2L -> "jj", 4L -> "kk", 6L -> "ll").toSnapshotProposals
          val peersProposals = Map(
            Id("a") -> Map(2L -> "gg", 4L -> "hh", 6L -> "zz"),
            Id("b") -> Map(2L -> "gg", 4L -> "hh", 6L -> "zz"),
            Id("c") -> Map(2L -> "gg", 4L -> "hh", 6L -> "yy"),
            Id("d") -> Map(2L -> "gg", 4L -> "hh", 6L -> "yy"),
            Id("e") -> Map(2L -> "aa", 4L -> "bb", 6L -> "xx"),
            Id("f") -> Map(2L -> "mm", 4L -> "nn", 6L -> "xx"),
            Id("g") -> Map(2L -> "aa", 4L -> "bb", 6L -> "aa"),
            Id("h") -> Map(2L -> "mm", 4L -> "nn", 6L -> "cc")
          ).toSnapshotProposals

          val result = Map(2L -> "gg", 4L -> "hh", 6L -> "xx")
          chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
        }
      }

      "and there is a different trust across the peers" - {
        "returns the proposal weighted by proposers' trust" in {
          val peersCache =
            List("z", "a", "b", "c").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
          val trustOwn = SortedMap[Id, Double](Id("z") -> 0.8, Id("a") -> 0.6, Id("b") -> 0.1, Id("c") -> 0.1)
          val trustA = SortedMap[Id, Double](Id("z") -> 1, Id("a") -> 0.2, Id("b") -> -0.5, Id("c") -> 0.1)
          val trustB = SortedMap[Id, Double](Id("z") -> 0.3, Id("a") -> 0.4, Id("b") -> 0.1, Id("c") -> 1)
          val trustC = SortedMap[Id, Double](Id("z") -> 1, Id("a") -> -1, Id("b") -> 0.1, Id("c") -> -1)

          val createdSnapshots = Map(2L -> "gg", 4L -> "hh", 6L -> "ii").mapValues(SnapshotProposal(_, trustOwn))
          val peersProposals = InvertedMap(
            Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz", 8L -> "dd").mapValues(SnapshotProposal(_, trustA)),
            Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trustB)),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd").mapValues(SnapshotProposal(_, trustC))
          )

          val result = Map(2L -> "gg", 4L -> "hh", 6L -> "ii")

          chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
        }

        "with sum of trust all yielding 0.0 returns the proposal weighted by ratio of occurrences" in {
          val peersCache =
            List("z", "a", "b", "c").map(Id(_)).map(_ -> NonEmptyList.one(MajorityHeight.genesis)).toMap
          val trustOwn = SortedMap[Id, Double](Id("z") -> 0.8, Id("a") -> 0.4, Id("b") -> 0.1, Id("c") -> 0.1)
          val trustA = SortedMap[Id, Double](Id("z") -> -0.4, Id("a") -> 0.2, Id("b") -> -0.3, Id("c") -> 0.1)
          val trustB = SortedMap[Id, Double](Id("z") -> -0.4, Id("a") -> 0.4, Id("b") -> 0.1, Id("c") -> -0.1)
          val trustC = SortedMap[Id, Double](Id("z") -> 0.0, Id("a") -> -1, Id("b") -> 0.1, Id("c") -> -0.1)

          val createdSnapshots = Map(2L -> "gg", 4L -> "hh", 6L -> "ii").mapValues(SnapshotProposal(_, trustOwn))
          val peersProposals = InvertedMap(
            Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz", 8L -> "dd").mapValues(SnapshotProposal(_, trustA)),
            Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trustB)),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd").mapValues(SnapshotProposal(_, trustC))
          )

          val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

          chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
        }
      }
    }

    "should take into account the joining height" in {
      val peersCache = Map(
        Id("z") -> NonEmptyList.one(MajorityHeight(Some(0L), None)),
        Id("a") -> NonEmptyList.one(MajorityHeight(Some(0L), None)),
        Id("b") -> NonEmptyList.one(MajorityHeight(Some(0L), None)),
        Id("c") -> NonEmptyList.one(MajorityHeight(Some(2L), None)),
        Id("d") -> NonEmptyList.one(MajorityHeight(Some(6L), None)),
        Id("e") -> NonEmptyList.one(MajorityHeight(Some(6L), None)),
        Id("f") -> NonEmptyList.one(MajorityHeight(Some(6L), None))
      )
      val createdSnapshots = Map(2L -> "xx", 4L -> "bb", 6L -> "cc", 8L -> "dd").toSnapshotProposals
      val peersProposals = Map(
        Id("a") -> Map(2L -> "xx", 4L -> "bb", 6L -> "cc", 8L -> "dd", 10L -> "ee"),
        Id("b") -> Map(2L -> "xx", 4L -> "bb", 6L -> "cc", 8L -> "dd", 10L -> "ee"),
        Id("c") -> Map(2L -> "aa", 4L -> "xx", 6L -> "cc", 8L -> "dd"),
        Id("d") -> Map(2L -> "aa", 8L -> "dd", 10L -> "ee"),
        Id("e") -> Map(2L -> "aa", 8L -> "dd", 10L -> "ee"),
        Id("f") -> Map(2L -> "aa", 8L -> "dd", 10L -> "ee")
      ).toSnapshotProposals

      val result = Map(2L -> "xx", 4L -> "bb", 6L -> "cc", 8L -> "dd")

      chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
    }

    "should take into account the leaving height" in {
      val peersCache = Map(
        Id("z") -> NonEmptyList.one(MajorityHeight(Some(0L), Some(4L))),
        Id("a") -> NonEmptyList.one(MajorityHeight(Some(0L), Some(8L))),
        Id("b") -> NonEmptyList.one(MajorityHeight(Some(0L), Some(8L))),
        Id("c") -> NonEmptyList.one(MajorityHeight(Some(2L), Some(10L))),
        Id("d") -> NonEmptyList.one(MajorityHeight(Some(6L), None))
      )
      val createdSnapshots = Map(2L -> "aa", 4L -> "bb").toSnapshotProposals
      val peersProposals = Map(
        Id("a") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd"),
        Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd"),
        Id("c") -> Map(4L -> "xx", 6L -> "cc", 8L -> "dd", 10L -> "aa"),
        Id("d") -> Map(8L -> "dd", 10L -> "ee", 12L -> "ff")
      ).toSnapshotProposals

      val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd", 10L -> "aa", 12L -> "ff")

      chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
    }

    "should take into account rejoins" in {
      val peersCache = Map(
        Id("z") -> NonEmptyList.of[MajorityHeight](MajorityHeight(Some(0L), Some(2L)), MajorityHeight(Some(4L), None)),
        Id("a") -> NonEmptyList.of[MajorityHeight](MajorityHeight(Some(0L), Some(8L))),
        Id("b") -> NonEmptyList
          .of[MajorityHeight](MajorityHeight(Some(4L), Some(6L)), MajorityHeight(Some(10L), Some(12L))),
        Id("c") -> NonEmptyList.of[MajorityHeight](MajorityHeight(Some(2L), Some(10L))),
        Id("d") -> NonEmptyList.of[MajorityHeight](MajorityHeight(Some(6L), None))
      )
      val createdSnapshots = Map(2L -> "aa", 6L -> "cc", 8L -> "dd", 10L -> "aa", 12L -> "dd").toSnapshotProposals
      val peersProposals = Map(
        Id("a") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd"),
        Id("b") -> Map(6L -> "cc", 12L -> "dd"),
        Id("c") -> Map(4L -> "xx", 6L -> "cc", 8L -> "dd", 10L -> "aa"),
        Id("d") -> Map(8L -> "dd", 10L -> "ee", 12L -> "ff")
      ).toSnapshotProposals

      val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd", 10L -> "aa", 12L -> "dd")

      chooseMajorityState(createdSnapshots, peersProposals, peersCache) shouldBe result
    }
  }

  private def chooseMajorityState(
    createdSnapshots: SnapshotProposalsAtHeight,
    peersProposals: PeersProposals,
    peersCache: PeersCache
  ): SnapshotsAtHeight = {
    val chooser = MajorityStateChooser(ownId)

    chooser.chooseMajorityState(
      createdSnapshots,
      peersProposals,
      peersCache
    )
  }
}
