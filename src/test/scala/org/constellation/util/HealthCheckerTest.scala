package org.constellation.util
import java.net.SocketException

import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.p2p.{Cluster, DownloadProcess}
import org.constellation.primitives.Schema.{Id, NodeState, NodeType}
import org.constellation.storage.RecentSnapshot
import org.constellation.util.HealthChecker.compareSnapshotState
import org.constellation.{ConstellationConcurrentEffect, DAO, Fixtures, ProcessingConfig}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FunSpecLike, Matchers}

class HealthCheckerTest
    extends FunSpecLike
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar {

  val dao: DAO = mock[DAO]
  dao.id shouldReturn Fixtures.id
  dao.processingConfig shouldReturn ProcessingConfig()

  val downloadProcess: DownloadProcess = mock[DownloadProcess]

  val healthChecker =
    new HealthChecker[IO](dao, downloadProcess)(ConstellationConcurrentEffect.global, Slf4jLogger.getLogger[IO])

  describe("compareSnapshotState util function") {

    val node1 = Id("node1") -> List(4, 3, 2, 1).map(i => RecentSnapshot(s"$i", i))
    val node2 = Id("node2") -> List(4, 3, 2, 1).map(i => RecentSnapshot(s"$i", i))
    val state = List(node1, node2)
    val ids = List(Id("node1"), Id("node2"))

    it("should return empty list hashes to be deleted but not below given height") {
      val ownSnapshots = List(6, 5, 4, 3).map(i => RecentSnapshot(s"$i", i))

      val diff = compareSnapshotState(ownSnapshots, state)
      healthChecker.shouldReDownload(ownSnapshots, diff) shouldBe false
    }

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
        SnapshotDiff(
          List(RecentSnapshot("someSnap", height)),
          List(RecentSnapshot("someSnap", height)),
          List(Id("peer"))
        )

      healthChecker.shouldReDownload(ownSnapshots, diff) shouldBe true
    }
    it("should return true when there are snaps to delete and nothing to download") {
      val diff =
        SnapshotDiff(List(RecentSnapshot("someSnap", height)), List.empty, List(Id("peer")))

      healthChecker.shouldReDownload(ownSnapshots, diff) shouldBe false
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

  describe("checkClusterConsistency function") {
    it("should return none when unable to get peers") {
      dao.readyPeers(NodeType.Full) shouldFailWith new SocketException("timeout")
      val result = healthChecker.checkClusterConsistency(List.empty).unsafeRunSync()
      result shouldBe None
    }
  }

  describe("startReDownload function") {
    it("should set node state in case of error") {
      dao.keyPair shouldReturn Fixtures.kp
      dao.cluster shouldReturn mock[Cluster[IO]]
      dao.cluster.getNodeState shouldReturnF NodeState.Ready
      val metrics = new Metrics(2)(dao)
      dao.metrics shouldReturn metrics

      downloadProcess.reDownload(List.empty, Map.empty) shouldReturn IO.raiseError(new SocketException("timeout"))
      downloadProcess.setNodeState(*) shouldReturnF Unit

      assertThrows[SocketException] {
        healthChecker.startReDownload(SnapshotDiff(List.empty, List.empty, List.empty), Map.empty).unsafeRunSync()
      }

      downloadProcess.setNodeState(NodeState.DownloadInProgress).wasCalled(once)
      downloadProcess.setNodeState(NodeState.Ready).wasCalled(once)
    }
  }

}
