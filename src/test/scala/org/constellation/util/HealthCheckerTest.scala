package org.constellation.util
import cats.effect.IO
import org.constellation.p2p.DownloadProcess
import org.constellation.{ConstellationConcurrentEffect, DAO, ProcessingConfig}
import org.constellation.primitives.Schema.Id
import org.constellation.storage.RecentSnapshot
import org.constellation.util.HealthChecker.compareSnapshotState
import org.mockito.IdiomaticMockito
import org.scalatest.{FunSpecLike, Matchers}

class HealthCheckerTest extends FunSpecLike with Matchers with IdiomaticMockito {

  val dao: DAO = mock[DAO]
  dao.processingConfig shouldReturn ProcessingConfig()
  val downloadProcess: DownloadProcess = mock[DownloadProcess]
  val healthChecker = new HealthChecker[IO](dao, downloadProcess)(ConstellationConcurrentEffect.global)

  describe("compareSnapshotState util function") {

    val node1 = Id("node1") -> List(4, 3, 2, 1).map(i => RecentSnapshot(s"$i", i))
    val node2 = Id("node2") -> List(4, 3, 2, 1).map(i => RecentSnapshot(s"$i", i))
    val state = List(node1, node2)
    val ids = List(Id("node1"), Id("node2"))

    it("should return part hashes to be deleted and to be downloaded") {
      val ownSnapshots = List(6, 5, 2, 1).map(i => RecentSnapshot(s"$i", i))

      compareSnapshotState(ownSnapshots, state) shouldBe SnapshotDiff(
        List(RecentSnapshot("6", 6), RecentSnapshot("5", 5)),
        List(RecentSnapshot("3", 3), RecentSnapshot("4", 4)),
        ids
      )

    }
    it("should return all snapshots to be deleted and download") {
      val ownSnapshots = List(7, 8, 6, 5).map(i => RecentSnapshot(s"$i", i))

      compareSnapshotState(ownSnapshots, state) shouldBe SnapshotDiff(
        List(RecentSnapshot("7", 7), RecentSnapshot("8", 8), RecentSnapshot("6", 6), RecentSnapshot("5", 5)),
        List(RecentSnapshot("1", 1), RecentSnapshot("2", 2), RecentSnapshot("3", 3), RecentSnapshot("4", 4)),
        ids
      )
    }

    it("should return no diff") {
      val ownSnapshots = List(4, 3, 2, 1).map(i => RecentSnapshot(s"$i", i))

      compareSnapshotState(ownSnapshots, state) shouldBe SnapshotDiff(List.empty, List.empty, ids)
    }
  }
  describe("shouldDownload function") {
    val height = 2
    val ownSnapshots = List(height).map(i => RecentSnapshot(s"$i", i))
    val interval = dao.processingConfig.snapshotHeightDelayInterval
    it("should return true when there are snaps to delete and to download") {
      val diff =
        SnapshotDiff(List(RecentSnapshot("someSnap", height)),
                     List(RecentSnapshot("someSnap", height)),
                     List(Id("peer")))

      healthChecker.shouldReDownload(ownSnapshots, diff) shouldBe true
    }
    it("should not throw return true when there are snaps to delete and to download") {
      val diff =
        SnapshotDiff(List(RecentSnapshot("someSnap", height)), List.empty, List(Id("peer")))

      healthChecker.shouldReDownload(ownSnapshots, diff) shouldBe true
    }

    it("should return false when height is too small") {
      val diff =
        SnapshotDiff(List.empty, List(RecentSnapshot("someSnap", height)), List(Id("peer")))

      healthChecker.shouldReDownload(ownSnapshots, diff) shouldBe false
    }

    it("should return true when height below interval") {
      val diff =
        SnapshotDiff(List.empty, List(RecentSnapshot("someSnap", height + (interval * 2))), List(Id("peer")))

      healthChecker.shouldReDownload(ownSnapshots, diff) shouldBe true
    }

  }

}
