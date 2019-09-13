package org.constellation.util

import org.constellation.primitives.Schema.Id
import org.constellation.storage.RecentSnapshot
import org.constellation.util.HealthChecker.{chooseSnapshotAndNodeIds, compareSnapshotState}
import org.mockito.ArgumentMatchersSugar
import org.scalatest.{FunSpecLike, Matchers}

class HealthCheckerChoseMajorTest extends FunSpecLike with ArgumentMatchersSugar with Matchers {

  describe("chooseSnapshotAndNodeIds function") {
    it("should return snapshot that is accepted by at least 51 percent") {

      val clusterSnapshots = List(
        (Id("node1"), List(2, 0).map(i => RecentSnapshot(s"$i", i))),
        (Id("node2"), List.empty),
        (Id("node3"), List(4, 2, 0).map(i => RecentSnapshot(s"$i", i))),
        (Id("node4"), List(4, 2, 0).map(i => RecentSnapshot(s"$i", i)))
      )

      chooseSnapshotAndNodeIds(clusterSnapshots) shouldBe
        Some(RecentSnapshot("2", 2), Set(Id("node1"), Id("node3"), Id("node4")))
    }
  }

  describe("compareSnapshotState function") {
    it("should return snapshots to download when the difference is lower than 10") {
      val node1 = Id("node1") -> List(
        RecentSnapshot("foo", 8),
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )
      val node2 = Id("node2") -> List(
        RecentSnapshot("bar", 8),
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )
      val node3 = Id("node3") -> List(
        RecentSnapshot("baz", 8),
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )
      val node4 = Id("node2") -> List(RecentSnapshot("2", 2), RecentSnapshot("0", 0))
      val state = List(node1, node2, node3, node4)

      val ownSnapshots = List(2, 0).map(i => RecentSnapshot(s"$i", i))

      compareSnapshotState((Id("ownNode"), ownSnapshots), state) shouldBe SnapshotDiff(
        List(),
        List(RecentSnapshot("6", 6), RecentSnapshot("4", 4)),
        List(Id("node1"), Id("node2"), Id("node3"))
      )
    }

    it("should return empty list to download when candidate doesn't exist") {
      val node1 = Id("node1") -> List(
        RecentSnapshot("foo", 8),
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )
      val node2 = Id("node2") -> List(
        RecentSnapshot("bar", 8),
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )
      val node3 = Id("node3") -> List(
        RecentSnapshot("baz", 8),
        RecentSnapshot("6", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )
      val node4 = Id("node2") -> List(RecentSnapshot("2", 2), RecentSnapshot("0", 0))
      val node5 = Id("node2") -> List(RecentSnapshot("2", 2), RecentSnapshot("0", 0))
      val state = List(node1, node2, node3, node4, node5)

      val ownSnapshots = List(2, 0).map(i => RecentSnapshot(s"$i", i))

      compareSnapshotState((Id("ownNode"), ownSnapshots), state) shouldBe SnapshotDiff(
        List(),
        List(),
        List(Id("node1"), Id("node2"), Id("node3"), Id("ownNode"))
      )
    }

    it("should empty list to download when candidate doesn't exist") {
      val node1 = Id("node1") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i))
      val node2 = Id("node2") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i))
      val state = List(node1, node2)

      val ownSnapshots = List(0, 2).map(i => RecentSnapshot(s"$i", i))

      compareSnapshotState((Id("ownNode"), ownSnapshots), state) shouldBe SnapshotDiff(
        List(),
        List(RecentSnapshot("6", 6), RecentSnapshot("4", 4)),
        List(Id("node1"), Id("node2"))
      )
    }

    it("aaa") {
      val node1 = Id("node1") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i))
      val node2 = Id("node2") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i))
      val state = List(node1, node2)

      val ownSnapshots = List(0, 2).map(i => RecentSnapshot(s"$i", i))

      compareSnapshotState((Id("ownNode"), ownSnapshots), state) shouldBe SnapshotDiff(
        List(),
        List(RecentSnapshot("6", 6), RecentSnapshot("4", 4)),
        List(Id("node1"), Id("node2"))
      )
    }

    it("yyyy") {
      val node1 = Id("node1") -> List(
        RecentSnapshot("foo", 8),
        RecentSnapshot("bar", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )
      val node2 = Id("node2") -> List(
        RecentSnapshot("foo", 8),
        RecentSnapshot("bar", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )
      val node3 = Id("node3") -> List(
        RecentSnapshot("foo", 8),
        RecentSnapshot("biz", 6),
        RecentSnapshot("4", 4),
        RecentSnapshot("2", 2),
        RecentSnapshot("0", 0)
      )
      val state = List(node1, node2, node3)

      val ownSnapshots = List(2, 0).map(i => RecentSnapshot(s"$i", i))

      compareSnapshotState((Id("ownNode"), ownSnapshots), state) shouldBe SnapshotDiff(
        List(),
        List(RecentSnapshot("foo", 8), RecentSnapshot("bar", 6), RecentSnapshot("4", 4)),
        List(Id("node1"))
      )
    }
  }
}
