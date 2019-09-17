package org.constellation.util

import cats.effect.{ContextShift, IO}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext
import org.constellation.primitives.Schema.Id
import org.constellation.storage.RecentSnapshot
import org.mockito.ArgumentMatchersSugar
import org.scalatest.{FunSpecLike, Matchers}

class MajorityStateChooserTest extends FunSpecLike with ArgumentMatchersSugar with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val majorityState = new MajorityStateChooser[IO]

  describe("Should return correct major state") {
    it("after receiving the snapshots") {
      val node1 = Id("node1") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i))
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshots in reverse order from first node") {
      val node1 = Id("node1") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i))
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshots in reverse order from nodes") {
      val node1 = Id("node1") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i))
      val node2 = Id("node2") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshot in a mixed order from nodes") {
      val node1 = Id("node1") -> List(2, 0, 6, 4).map(i => RecentSnapshot(s"$i", i))
      val node2 = Id("node2") -> List(4, 6, 2, 0).map(i => RecentSnapshot(s"$i", i))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the major state is lower than own snapshots") {
      val node1 = Id("node1") -> List(2, 0).map(i => RecentSnapshot(s"$i", i))
      val node2 = Id("node2") -> List(2, 0).map(i => RecentSnapshot(s"$i", i))
      val ownNode = Id("ownNode") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )

      result._2.subsetOf(Set(Id("node1"), Id("node2"), Id("ownNode"))) shouldBe true
    }

    it("after receiving the snapshots with a difference greater than 10") {
      val node1 = Id("node1") -> List(14, 12, 10, 8, 6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i))
      val node2 = Id("node2") -> List(2, 0).map(i => RecentSnapshot(s"$i", i))
      val ownNode = Id("ownNode") -> List(0, 2, 4).map(i => RecentSnapshot(s"$i", i))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("14", 14),
        RecentSnapshot("12", 12),
        RecentSnapshot("10", 10),
        RecentSnapshot("8", 8),
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )

      result._2.subsetOf(Set(Id("node1"))) shouldBe true
    }

    it("after receiving empty list from one node") {
      val node1 = Id("node1") -> List()
      val node2 = Id("node2") -> List(2, 0).map(i => RecentSnapshot(s"$i", i))
      val ownNode = Id("ownNode") -> List(0, 2, 4, 6, 8).map(i => RecentSnapshot(s"$i", i))

      val result =
        majorityState.chooseMajorityState(List(node1, node2, ownNode), maxOrZero(ownNode._2)).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
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

    it("after receiving snapshots with different hashes") {
      val clusterSnapshots = List(
        (
          Id("node1"),
          List(
            RecentSnapshot("foo", 4),
            RecentSnapshot("bar", 2),
            RecentSnapshot("0", 0)
          )
        ),
        (
          Id("node2"),
          List(
            RecentSnapshot("foo", 4),
            RecentSnapshot("bar", 2),
            RecentSnapshot("0", 0)
          )
        ),
        (
          Id("node3"),
          List(
            RecentSnapshot("foo", 4),
            RecentSnapshot("bar", 2),
            RecentSnapshot("0", 0)
          )
        ),
        (
          Id("node4"),
          List(
            RecentSnapshot("6", 6),
            RecentSnapshot("4", 4),
            RecentSnapshot("biz", 2),
            RecentSnapshot("0", 0)
          )
        )
      )

      val result = majorityState.chooseMajorityState(clusterSnapshots, 4).value.unsafeRunSync().get

      result._1 shouldBe List(
        RecentSnapshot("foo", 4),
        RecentSnapshot("bar", 2),
        RecentSnapshot("0", 0)
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
