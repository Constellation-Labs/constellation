package org.constellation.util
import java.net.SocketException

import cats.effect.IO
import org.constellation.consensus.ConsensusManager
import org.constellation.p2p.{Cluster, DownloadProcess, SetStateResult}
import org.constellation.primitives.ConcurrentTipService
import org.constellation.primitives.Schema.{NodeState, NodeType}
import org.constellation.schema.Id
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
      IO.contextShift(ConstellationExecutionContext.bounded)
    )

  describe("clear stale tips") {
    it("should run tips removal with max height of minimum nodes required to make consensus") {

      concurrentTipService.clearStaleTips(*) shouldReturn IO.unit

      val cluster = List(
        (
          Id("1"),
          List(
            RecentSnapshot("snap4", 4, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
          )
        ),
        (
          Id("2"),
          List(
            RecentSnapshot("snap4", 6, Map.empty),
            RecentSnapshot("snap4", 4, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
          )
        ),
        (
          Id("3"),
          List(
            RecentSnapshot("snap4", 6, Map.empty),
            RecentSnapshot("snap4", 4, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
          )
        ),
        (
          Id("4"),
          List(
            RecentSnapshot("snap8", 8, Map.empty),
            RecentSnapshot("snap6", 6, Map.empty),
            RecentSnapshot("snap4", 4, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
          )
        ),
        (
          Id("5"),
          List(
            RecentSnapshot("snap8", 8, Map.empty),
            RecentSnapshot("snap6", 6, Map.empty),
            RecentSnapshot("snap4", 4, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
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
        (
          Id("1"),
          List(
            RecentSnapshot("snap4", 4, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
          )
        ),
        (
          Id("2"),
          List(
            RecentSnapshot("snap4", 6, Map.empty),
            RecentSnapshot("snap4", 4, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
          )
        )
      )
      healthChecker.clearStaleTips(cluster).unsafeRunSync()
      concurrentTipService.clearStaleTips(*).wasNever(called)
    }
    it("should not run tips removal when there are empty heights at least big as minimum facilitators") {

      concurrentTipService.clearStaleTips(*) shouldReturn IO.unit

      val cluster = List(
        (
          Id("1"),
          List(
            RecentSnapshot("snap4", 4, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
          )
        ),
        (
          Id("2"),
          List(
            RecentSnapshot("snap4", 4, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
          )
        ),
        (
          Id("2"),
          List(
            RecentSnapshot("snap4", 6, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
          )
        ),
        (
          Id("2"),
          List(
            RecentSnapshot("snap4", 6, Map.empty),
            RecentSnapshot("snap2", 2, Map.empty),
            RecentSnapshot("snap0", 0, Map.empty)
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

  describe("checkClusterConsistency function") {
    it("should return none when unable to get peers") {
      dao.readyPeers(NodeType.Full) shouldFailWith new SocketException("timeout")
      val result = healthChecker.checkClusterConsistency(List.empty).unsafeRunSync()
      result shouldBe ()
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
