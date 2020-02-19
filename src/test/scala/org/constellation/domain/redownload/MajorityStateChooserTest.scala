package org.constellation.domain.redownload

import org.constellation.schema.Id
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfterEach, FreeSpec, Matchers}

class MajorityStateChooserTest
    extends FreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {

  "choose majority state" - {
    "returns Map.empty" - {
      "if there are no created snapshots and peers proposals" - {
        chooseMajorityState(Map.empty, Map.empty) shouldBe Map.empty
      }

      "if there are no created snapshots and there are 2 peers (but only one with snapshot)" in {
        val createdSnapshots = Map.empty[Long, String]
        val peersProposals = Map(
          Id("a") -> Map(2L -> "aa"),
          Id("b") -> Map.empty[Long, String]
        )

        chooseMajorityState(createdSnapshots, peersProposals) shouldBe Map.empty
      }

      "if there is one created snapshot and there are 2 peers without snapshots" in {
        val createdSnapshots = Map(2L -> "aa")
        val peersProposals = Map(
          Id("a") -> Map.empty[Long, String],
          Id("b") -> Map.empty[Long, String]
        )

        chooseMajorityState(createdSnapshots, peersProposals) shouldBe Map.empty
      }
    }

    "if there is only one height" - {
      "returns Map with majority snapshot at this height" - {
        "if the snapshot is in the majority (more than 50%)" in {
          val createdSnapshots = Map(2L -> "aa")
          val peersProposals = Map(
            Id("a") -> Map(2L -> "bb"),
            Id("b") -> Map(2L -> "bb")
          )
          val result = Map(2L -> "bb")

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }
      }
    }

    "if there are more than one heights" - {
      "returns majority snapshot at each height if there is a clear majority (more than 50%)" in {
        val createdSnapshots = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")
        val peersProposals = Map(
          Id("a") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc", 8L -> "dd"),
          Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "ccc", 8L -> "ddd")
        )
        val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

        chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
      }
    }

    "if there is 50%-50% split" - {
      "returns the first hash in alphabetical order" in {
        val createdSnapshots = Map(2L -> "xx", 4L -> "yy", 6L -> "zz")
        val peersProposals = Map(
          Id("a") -> Map(2L -> "xx", 4L -> "yy", 6L -> "zz"),
          Id("b") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc"),
          Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc")
        )
        val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

        chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
      }

      "does not return consistent continuation but treats each height individually" in {
        val createdSnapshots = Map(2L -> "xx", 4L -> "dd", 6L -> "zz")
        val peersProposals = Map(
          Id("a") -> Map(2L -> "xx", 4L -> "dd", 6L -> "zz"),
          Id("b") -> Map(2L -> "aa", 4L -> "ee", 6L -> "cc"),
          Id("c") -> Map(2L -> "aa", 4L -> "ee", 6L -> "cc")
        )
        val result = Map(2L -> "aa", 4L -> "dd", 6L -> "cc")

        chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
      }
    }

    "if there is no clear majority (less than 50%)" - {
      "and each proposal has the same quantity" - {
        "returns the first hash in alphabetical order" in {
          val createdSnapshots = Map(2L -> "aa", 4L -> "ee", 6L -> "ii")
          val peersProposals = Map(
            Id("a") -> Map(2L -> "dd", 4L -> "bb", 6L -> "ff"),
            Id("b") -> Map(2L -> "gg", 4L -> "hh", 6L -> "cc")
          )
          val result = Map(2L -> "aa", 4L -> "bb", 6L -> "cc")

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }
      }

      "and at least one proposal has bigger quantity" - {
        "returns the proposal with the most quantity" in {
          val createdSnapshots = Map(2L -> "jj", 4L -> "kk", 6L -> "ll")
          val peersProposals = Map(
            Id("a") -> Map(2L -> "gg", 4L -> "hh", 6L -> "zz"),
            Id("b") -> Map(2L -> "gg", 4L -> "hh", 6L -> "ii"),
            Id("c") -> Map(2L -> "aa", 4L -> "bb", 6L -> "cc"),
            Id("d") -> Map(2L -> "mm", 4L -> "nn", 6L -> "oo")
          )
          val result = Map(2L -> "gg", 4L -> "hh", 6L -> "cc")

          chooseMajorityState(createdSnapshots, peersProposals) shouldBe result
        }
      }

    }
  }

  private def chooseMajorityState(
    createdSnapshots: Map[Long, String],
    peersProposals: Map[Id, Map[Long, String]]
  ) = {
    val chooser = MajorityStateChooser()

    chooser.chooseMajorityState(createdSnapshots, peersProposals)
  }

}
