package org.constellation.util
import org.constellation.primitives.Schema.Id
import org.constellation.util.HealthChecker.compareSnapshotState
import org.scalatest.{FunSpecLike, Matchers}

class HealthCheckerTest extends FunSpecLike with Matchers {

  describe("compareSnapshotState util function") {
    val node1 = Id("node1") -> List(4, 3, 2, 1).map(_.toString)
    val node2 = Id("node2") -> List(4, 3, 2, 1).map(_.toString)
    val state = List(node1, node2)
    val ids = Set(Id("node1"), Id("node2"))

    it("should return part hashes to be deleted and to be downloaded") {
      val ownSnapshots = List(6, 5, 2, 1).map(_.toString)

      compareSnapshotState(ownSnapshots, state) shouldBe SnapshotDiff(List("6", "5"), List("3", "4"), ids)

    }
    it("should return all snapshots to be deleted and download") {
      val ownSnapshots = List(7, 8, 6, 5).map(_.toString)

      compareSnapshotState(ownSnapshots, state) shouldBe SnapshotDiff(List("7", "8", "6", "5"),
                                                                      List("1", "2", "3", "4"),
                                                                      ids)
    }

    it("should return no diff") {
      val ownSnapshots = List(4, 3, 2, 1).map(_.toString)

      compareSnapshotState(ownSnapshots, state) shouldBe SnapshotDiff(List.empty, List.empty, ids)
    }
  }

}
