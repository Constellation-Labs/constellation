package org.constellation.util
import java.net.SocketException

import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.ConsensusManager
import org.constellation.p2p.{Cluster, DownloadProcess, SetStateResult}
import org.constellation.primitives.ConcurrentTipService
import org.constellation.primitives.Schema.{NodeState, NodeType}
import org.constellation.domain.schema.Id
import org.constellation.storage.RecentSnapshot
import org.constellation.{ConstellationExecutionContext, DAO, Fixtures, ProcessingConfig}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Matchers}

class HealthCheckerTest
    extends FunSpecLike
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with BeforeAndAfterEach
    with ArgumentMatchersSugar {

  val dao: DAO = mock[DAO]
  dao.id shouldReturn Fixtures.id
  dao.processingConfig shouldReturn ProcessingConfig()

  val downloadProcess: DownloadProcess[IO] = mock[DownloadProcess[IO]]
  val consensusManager: ConsensusManager[IO] = mock[ConsensusManager[IO]]
  val concurrentTipService: ConcurrentTipService[IO] = mock[ConcurrentTipService[IO]]
  val cluster: Cluster[IO] = mock[Cluster[IO]]
  val majorState: MajorityStateChooser[IO] = mock[MajorityStateChooser[IO]]

  val healthChecker =
    new HealthChecker[IO](
      dao,
      concurrentTipService,
      consensusManager,
      IO.contextShift(ConstellationExecutionContext.bounded),
      downloadProcess,
      cluster,
      majorState
    )(
      IO.ioConcurrentEffect(IO.contextShift(ConstellationExecutionContext.bounded)),
      Slf4jLogger.getLogger[IO],
      IO.contextShift(ConstellationExecutionContext.bounded)
    )

  describe("compareSnapshotState util function") {
    it("should return empty list to be deleted but not below given height") {
      val ownSnapshots = List(6, 5, 4, 3).map(i => RecentSnapshot(s"$i", i))
      val majorState = (List(7, 6, 5, 4, 3).map(i => RecentSnapshot(s"$i", i)), Set(Id("node1")))

      val diff = healthChecker.compareSnapshotState(majorState, ownSnapshots).unsafeRunSync()

      diff.snapshotsToDelete shouldBe List()
      diff.snapshotsToDownload shouldBe List(RecentSnapshot("7", 7))
      diff.peers.size shouldBe 1
    }

    it("should return no diff") {
      val ownSnapshots = List(4, 3, 2, 1).map(i => RecentSnapshot(s"$i", i))
      val majorState = (List(4, 3, 2, 1).map(i => RecentSnapshot(s"$i", i)), Set[Id]())

      val diff = healthChecker.compareSnapshotState(majorState, ownSnapshots).unsafeRunSync()

      diff.snapshotsToDelete shouldBe List()
      diff.snapshotsToDownload shouldBe List()
    }
  }

  describe("clear stale tips") {
    it("should run tips removal with max height of minimum nodes required to make consensus") {

      concurrentTipService.clearStaleTips(*) shouldReturn IO.unit

      val cluster = List(
        (Id("1"), List(RecentSnapshot("snap4", 4), RecentSnapshot("snap2", 2), RecentSnapshot("snap0", 0))),
        (
          Id("2"),
          List(
            RecentSnapshot("snap4", 6),
            RecentSnapshot("snap4", 4),
            RecentSnapshot("snap2", 2),
            RecentSnapshot("snap0", 0)
          )
        ),
        (
          Id("3"),
          List(
            RecentSnapshot("snap4", 6),
            RecentSnapshot("snap4", 4),
            RecentSnapshot("snap2", 2),
            RecentSnapshot("snap0", 0)
          )
        ),
        (
          Id("4"),
          List(
            RecentSnapshot("snap8", 8),
            RecentSnapshot("snap6", 6),
            RecentSnapshot("snap4", 4),
            RecentSnapshot("snap2", 2),
            RecentSnapshot("snap0", 0)
          )
        ),
        (
          Id("5"),
          List(
            RecentSnapshot("snap8", 8),
            RecentSnapshot("snap6", 6),
            RecentSnapshot("snap4", 4),
            RecentSnapshot("snap2", 2),
            RecentSnapshot("snap0", 0)
          )
        ),
        (
          Id("6"),
          List.empty
        )
      )
      healthChecker.clearStaleTips(cluster).unsafeRunSync()
      concurrentTipService.clearStaleTips(6 + healthChecker.snapshotHeightInterval).wasCalled(once)
    }
    it("should not run tips removal when there is not enough data") {

      concurrentTipService.clearStaleTips(*) shouldReturn IO.unit

      val cluster = List(
        (Id("1"), List(RecentSnapshot("snap4", 4), RecentSnapshot("snap2", 2), RecentSnapshot("snap0", 0))),
        (
          Id("2"),
          List(
            RecentSnapshot("snap4", 6),
            RecentSnapshot("snap4", 4),
            RecentSnapshot("snap2", 2),
            RecentSnapshot("snap0", 0)
          )
        )
      )
      healthChecker.clearStaleTips(cluster).unsafeRunSync()
      concurrentTipService.clearStaleTips(*).wasNever(called)
    }
    it("should not run tips removal when there are empty heights at least big as minimum facilitators") {

      concurrentTipService.clearStaleTips(*) shouldReturn IO.unit

      val cluster = List(
        (Id("1"), List(RecentSnapshot("snap4", 4), RecentSnapshot("snap2", 2), RecentSnapshot("snap0", 0))),
        (
          Id("2"),
          List(
            RecentSnapshot("snap4", 4),
            RecentSnapshot("snap2", 2),
            RecentSnapshot("snap0", 0)
          )
        ),
        (
          Id("2"),
          List(
            RecentSnapshot("snap4", 6),
            RecentSnapshot("snap2", 2),
            RecentSnapshot("snap0", 0)
          )
        ),
        (
          Id("2"),
          List(
            RecentSnapshot("snap4", 6),
            RecentSnapshot("snap2", 2),
            RecentSnapshot("snap0", 0)
          )
        ),
        (
          Id("3"),
          List.empty
        ),
        (
          Id("4"),
          List.empty
        )
      )
      healthChecker.clearStaleTips(cluster).unsafeRunSync()
      concurrentTipService.clearStaleTips(*).wasNever(called)
    }
    it("should not run tips removal when cluster info is empty") {
      reset(concurrentTipService)
      concurrentTipService.clearStaleTips(*) shouldReturn IO.unit

      healthChecker.clearStaleTips(List.empty).unsafeRunSync()
      concurrentTipService.clearStaleTips(*).wasNever(called)
    }

    it("should not run tips removal when cluster height info is missing") {
      reset(concurrentTipService)
      concurrentTipService.clearStaleTips(*) shouldReturn IO.unit

      val cluster = List((Id("1"), List()), (Id("1"), List()), (Id("1"), List()))

      healthChecker.clearStaleTips(cluster).unsafeRunSync()
      concurrentTipService.clearStaleTips(*).wasNever(called)
    }

  }

  describe("shouldDownload function") {

    val height = 2
    val ownSnapshots = List(height).map(i => RecentSnapshot(s"$i", i))
    val interval = healthChecker.snapshotHeightRedownloadDelayInterval

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
        SnapshotDiff(List.empty, List(RecentSnapshot(height.toString, height)), List(Id("peer")))

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
      cluster.compareAndSet(NodeState.validDuringDownload, NodeState.Ready) shouldReturnF SetStateResult(
        NodeState.Ready,
        isNewSet = true
      )
      cluster.compareAndSet(NodeState.validForRedownload, NodeState.DownloadInProgress) shouldReturnF SetStateResult(
        NodeState.Ready,
        isNewSet = true
      )
      dao.keyPair shouldReturn Fixtures.kp
      consensusManager.terminateConsensuses() shouldReturnF Unit
      val metrics = new Metrics(2)(dao)
      dao.metrics shouldReturn metrics

      dao.terminateConsensuses shouldReturnF Unit

      downloadProcess.reDownload(List.empty, Map.empty) shouldReturn IO.raiseError(new SocketException("timeout"))

      assertThrows[SocketException] {
        healthChecker.startReDownload(SnapshotDiff(List.empty, List.empty, List.empty), Map.empty).unsafeRunSync()
      }

      cluster.compareAndSet(NodeState.validForRedownload, NodeState.DownloadInProgress).wasCalled(once)
      cluster.compareAndSet(NodeState.validDuringDownload, NodeState.Ready).wasCalled(once)
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    reset(concurrentTipService)
  }
}
