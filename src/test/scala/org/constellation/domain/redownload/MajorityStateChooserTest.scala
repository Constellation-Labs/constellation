package org.constellation.domain.redownload

import org.constellation.domain.redownload.MajorityStateChooser.SnapshotProposal
import org.constellation.domain.redownload.RedownloadService.{
  PeersProposals,
  SnapshotProposalsAtHeight,
  SnapshotsAtHeight
}
import org.constellation.schema.Id
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfterEach, FreeSpec, Matchers}

import scala.collection.SortedMap

class MajorityStateChooserTest
    extends FreeSpec
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
    val toSnapshotProposals = value.mapValues(_.mapValues(SnapshotProposal(_, SortedMap.empty)))
  }

  "choose majority state" - {
    "returns Map.empty" - {
      "if there are no created snapshots and peers proposals" - {
        chooseMajorityState(Map.empty, Map.empty) shouldBe Map.empty
      }

      "if there are no created snapshots and there are 2 peers (but only one with snapshot)" in {
        val createdSnapshots = Map.empty[Long, SnapshotProposal]
        val peersProposals = Map(
          Id("a") -> Map(2L -> "aa"),
          Id("b") -> Map.empty[Long, String]
        ).toSnapshotProposals

        chooseMajorityState(createdSnapshots, peersProposals) shouldBe Map.empty
      }

      "if there is one created snapshot and there are 2 peers without snapshots" in {
        val createdSnapshots = Map(2L -> "aa").toSnapshotProposals
        val peersProposals = Map(
          Id("a") -> Map.empty[Long, SnapshotProposal],
          Id("b") -> Map.empty[Long, SnapshotProposal]
        )

        chooseMajorityState(createdSnapshots, peersProposals) shouldBe Map.empty
      }
    }

    "if there is only one height" - {
      "and not all peers made snapshot" - {
        "returns empty Map without majority snapshot at this height" - {
          val createdSnapshots = Map(2L -> "aa").toSnapshotProposals
          val peersProposals = Map(
            Id("a") -> Map(2L -> "bb"),
            Id("b") -> Map(2L -> "bb"),
            Id("c") -> Map.empty[Long, String]
          ).toSnapshotProposals
          val result = Map.empty[Long, String]

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }
      }
    }

    "if there are more than one heights" - {
      "returns majority snapshot at each height if there is a clear majority (more than 50%)" in {
        val createdSnapshots = Map(2L -> "aa", 4L -> "bb", 6L -> "cc").toSnapshotProposals
        val peersProposals = Map(
          Id("a") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd"),
          Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "ccc", 8L -> "ddd")
        ).toSnapshotProposals
        val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

        chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
      }
    }

    "if there is 50%-50% split" - {
      "if there is no reputation" - {
        "returns the first hash in alphabetical order" in {
          val createdSnapshots = Map(2L -> "xx", 4L -> "yy", 6L -> "zz").toSnapshotProposals
          val peersProposals = Map(
            Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz"),
            Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc"),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc")
          ).toSnapshotProposals
          val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }

        "does not return consistent continuation but treats each height individually" in {
          val createdSnapshots = Map(2L -> "xx", 4L -> "dd", 6L -> "zz").toSnapshotProposals
          val peersProposals = Map(
            Id("a") -> Map(2L -> "xx", 4L -> "dd", 6L -> "zz"),
            Id("b") -> Map(2L -> "aa", 4L -> "ee", 6L -> "cc"),
            Id("c") -> Map(2L -> "aa", 4L -> "ee", 6L -> "cc")
          ).toSnapshotProposals
          val result = Map(2L -> "aa", 4L -> "dd", 6L -> "cc")

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }
      }

      "if there is reputation" - {
        "return the majority calculated by trust proposers" in {
          val trust = SortedMap[Id, Double](Id("z") -> 1, Id("a") -> 1, Id("b") -> 0.1, Id("c") -> 0.1)
          val createdSnapshots = Map(2L -> "xx", 4L -> "yy", 6L -> "zz").mapValues(SnapshotProposal(_, trust))
          val peersProposals = Map(
            Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz").mapValues(SnapshotProposal(_, trust)),
            Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trust)),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trust))
          )

          val result = Map(2L -> "xx", 4L -> "yy", 6L -> "zz")

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }
      }

      "if there is positive and negative reputation" - {
        "return the majority calculated by trust proposers" in {
          val trust = SortedMap[Id, Double](Id("z") -> 1, Id("a") -> -1, Id("b") -> -0.1, Id("c") -> -0.1)
          val createdSnapshots = Map(2L -> "xx", 4L -> "yy", 6L -> "zz").mapValues(SnapshotProposal(_, trust))
          val peersProposals = Map(
            Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz").mapValues(SnapshotProposal(_, trust)),
            Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trust)),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trust))
          )

          val result = Map(2L -> "xx", 4L -> "yy", 6L -> "zz")

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }
      }
    }

    "if there is no clear majority (less than 50%)" - {
      "and each proposal has the same quantity" - {
        "returns the first hash in alphabetical order" in {
          val createdSnapshots = Map(2L -> "aa", 4L -> "ee", 6L -> "ii").toSnapshotProposals
          val peersProposals = Map(
            Id("a") -> Map(2L -> "dd", 4L -> "bb", 6L -> "ff"),
            Id("b") -> Map(2L -> "gg", 4L -> "hh", 6L -> "cc")
          ).toSnapshotProposals
          val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }
      }

      "and at least one proposal has bigger quantity" - {
        "returns the proposal with the most quantity" in {
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
          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }
      }

      "and there is a different trust across the peers" - {
        "returns the proposal weighted by proposers' trust" in {
          val trustOwn = SortedMap[Id, Double](Id("z") -> 0.8, Id("a") -> 0.6, Id("b") -> 0.1, Id("c") -> 0.1)
          val trustA = SortedMap[Id, Double](Id("z") -> 1, Id("a") -> 0.2, Id("b") -> -0.5, Id("c") -> 0.1)
          val trustB = SortedMap[Id, Double](Id("z") -> 0.3, Id("a") -> 0.4, Id("b") -> 0.1, Id("c") -> 1)
          val trustC = SortedMap[Id, Double](Id("z") -> 1, Id("a") -> -1, Id("b") -> 0.1, Id("c") -> -1)

          val createdSnapshots = Map(2L -> "gg", 4L -> "hh", 6L -> "ii").mapValues(SnapshotProposal(_, trustOwn))
          val peersProposals = Map(
            Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz", 8L -> "dd").mapValues(SnapshotProposal(_, trustA)),
            Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trustB)),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd").mapValues(SnapshotProposal(_, trustC))
          )

          val result = Map(2L -> "gg", 4L -> "hh", 6L -> "ii")

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }

        "with sum of trust all yielding 0.0 returns the proposal weighted by ratio of occurrences" in {
          val trustOwn = SortedMap[Id, Double](Id("z") -> 0.8, Id("a") -> 0.4, Id("b") -> 0.1, Id("c") -> 0.1)
          val trustA = SortedMap[Id, Double](Id("z") -> -0.4, Id("a") -> 0.2, Id("b") -> -0.3, Id("c") -> 0.1)
          val trustB = SortedMap[Id, Double](Id("z") -> -0.4, Id("a") -> 0.4, Id("b") -> 0.1, Id("c") -> -0.1)
          val trustC = SortedMap[Id, Double](Id("z") -> 0.0, Id("a") -> -1, Id("b") -> 0.1, Id("c") -> -0.1)

          val createdSnapshots = Map(2L -> "gg", 4L -> "hh", 6L -> "ii").mapValues(SnapshotProposal(_, trustOwn))
          val peersProposals = Map(
            Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz", 8L -> "dd").mapValues(SnapshotProposal(_, trustA)),
            Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc").mapValues(SnapshotProposal(_, trustB)),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd").mapValues(SnapshotProposal(_, trustC))
          )

          val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }
      }
    }
  }

  private def chooseMajorityState(
    createdSnapshots: SnapshotProposalsAtHeight,
    peersProposals: PeersProposals
  ): SnapshotsAtHeight = {
    val chooser = MajorityStateChooser(ownId)

    chooser.chooseMajorityState(
      createdSnapshots,
      peersProposals
    )
  }
}
