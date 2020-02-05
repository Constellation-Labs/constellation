package org.constellation.util

import cats.effect.{ContextShift, IO}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext
import org.constellation.schema.Id
import org.constellation.storage.RecentSnapshot
import org.mockito.ArgumentMatchersSugar
import org.scalatest.{FunSpecLike, Matchers}
import org.scalatest.FreeSpec

class MajorityStateChooserFreeTest extends FreeSpec with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val majorityState = new MajorityStateChooser[IO]

  def toRecentSnapshot(i: Int) = RecentSnapshot(i.toString, i, Map.empty)
  def toRecentSnapshotWithPrefix(prefix: String)(i: Int) = RecentSnapshot(s"$prefix$i", i, Map.empty)

  "choose majority state" - {
    "when node creates a correct snapshot" - {
      "chooses own node if majority at given height is equal" in {
        val node1 = Id("node1") -> List(0, 2, 4, 6).map(toRecentSnapshot)
        val node2 = Id("node2") -> List(0, 2, 4, 6).map(toRecentSnapshot)

        val ownNode = Id("ownNode") -> List(0, 2).map(toRecentSnapshot)

        val height = 2L

        val result = majorityState.chooseMajorityState(List(node1, node2, ownNode), height).value.unsafeRunSync.get

        result._1 shouldBe List(2, 0).map(toRecentSnapshot)
      }
    }

    "when node creates an incorrect snapshot" - {
      "chooses all nodes who has the majority at given height if all of them have same amount of snapshots" in {
        val node1 = Id("node1") -> List(0, 2, 4, 6, 8).map(toRecentSnapshot)
        val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(toRecentSnapshot)
        val node3 = Id("node3") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6, 8).map(toRecentSnapshotWithPrefix("a")))

        val ownNode = Id("ownNode") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6).map(toRecentSnapshotWithPrefix("b")))

        val height = 6L

        val result = majorityState.chooseMajorityState(List(node1, node2, node3, ownNode), height).value.unsafeRunSync.get

        result._1 shouldBe List(8, 6, 4, 2, 0).map(toRecentSnapshot)
        result._2 shouldBe List(Id("node1"), Id("node2"))
      }

      "chooses the node with most snapshots who has the majority at given height" in {
        val node1 = Id("node1") -> List(0, 2, 4, 6, 8, 10).map(toRecentSnapshot)
        val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(toRecentSnapshot)
        val node3 = Id("node3") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6, 8).map(toRecentSnapshotWithPrefix("a")))

        val ownNode = Id("ownNode") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6).map(toRecentSnapshotWithPrefix("b")))

        val height = 6L

        val result = majorityState.chooseMajorityState(List(node1, node2, node3, ownNode), height).value.unsafeRunSync.get

        result._1 shouldBe List(10, 8, 6, 4, 2, 0).map(toRecentSnapshot)
        result._2 shouldBe List(Id("node1"))
      }

      "chooses the node with most snapshots and the highest snapshot who has the majority at given height" in {
        val node1 = Id("node1") -> List(2, 4, 6, 8, 10).map(toRecentSnapshot)
        val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(toRecentSnapshot)
        val node3 = Id("node3") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6, 8).map(toRecentSnapshotWithPrefix("a")))

        val ownNode = Id("ownNode") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6).map(toRecentSnapshotWithPrefix("b")))

        val height = 6L

        val result = majorityState.chooseMajorityState(List(node1, node2, node3, ownNode), height).value.unsafeRunSync.get

        result._1 shouldBe List(10, 8, 6, 4, 2).map(toRecentSnapshot)
        result._2 shouldBe List(Id("node1"))
      }
    }
  }

}

class MajorityStateChooserTest extends FunSpecLike with ArgumentMatchersSugar with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val majorityState = new MajorityStateChooser[IO]

  describe("Should return correct major state") {
    it("after receiving the snapshots") {
      val node1 = Id("node1") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i, Map.empty))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshots in reverse order from first node") {
      val node1 = Id("node1") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i, Map.empty))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshots in reverse order from nodes") {
      val node1 = Id("node1") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i, Map.empty))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshot in a mixed order from nodes") {
      val node1 = Id("node1") -> List(2, 0, 6, 4).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(4, 6, 2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i, Map.empty))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the major state is lower than own snapshots") {
      val node1 = Id("node1") -> List(2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"), Id("ownNode"))) shouldBe true
    }

    it("after receiving the snapshots with a difference greater than 10") {
      val node1 = Id("node1") -> List(14, 12, 10, 8, 6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2, 4).map(i => RecentSnapshot(s"$i", i, Map.empty))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("14", 14, Map.empty),
        RecentSnapshot("12", 12, Map.empty),
        RecentSnapshot("10", 10, Map.empty),
        RecentSnapshot("8", 8, Map.empty),
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      result._2.subsetOf(Set(Id("node1"))) shouldBe true
    }

    it("after receiving empty list from one node") {
      val node1 = Id("node1") -> List()
      val node2 = Id("node2") -> List(2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2, 4, 6, 8).map(i => RecentSnapshot(s"$i", i, Map.empty))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("8", 8, Map.empty),
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      result._2.subsetOf(Set(Id("node2"), Id("ownNode"))) shouldBe true
    }

    it("after receiving empty lists from all nodes") {
      val node1 = Id("node1") -> List()
      val node2 = Id("node2") -> List()
      val ownNode = Id("ownNode") -> List()

      val result = majorityState.chooseMajorityState(List(node1, node2, ownNode), 2).value.unsafeRunSync()

      result shouldBe None
    }

    it("after receiving not consistent lists from all nodes") {
      val node1 = Id("node1") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node3 = Id("node3") -> List(0, 2, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val nodeList = List(node1, node2, node3, ownNode)

      val result = majorityState.chooseMajorityState(nodeList, maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._2.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving snapshots with different hashes") {
      val clusterSnapshots = List(
        (
          Id("node1"),
          List(
            RecentSnapshot("foo", 4, Map.empty),
            RecentSnapshot("bar", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          )
        ),
        (
          Id("node2"),
          List(
            RecentSnapshot("foo", 4, Map.empty),
            RecentSnapshot("bar", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          )
        ),
        (
          Id("node3"),
          List(
            RecentSnapshot("foo", 4, Map.empty),
            RecentSnapshot("bar", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          )
        ),
        (
          Id("node4"),
          List(
            RecentSnapshot("6", 6, Map.empty),
            RecentSnapshot("4", 4, Map.empty),
            RecentSnapshot("biz", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          )
        )
      )

      val result = majorityState.chooseMajorityState(clusterSnapshots, 4).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("foo", 4, Map.empty),
        RecentSnapshot("bar", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"), Id("node3"))) shouldBe true
    }
  }

  private def maxOrZero(list: List[RecentSnapshot]): Long =
    list match {
      case Nil      => 0
      case nonEmpty => nonEmpty.map(_.height).max
    }
}
