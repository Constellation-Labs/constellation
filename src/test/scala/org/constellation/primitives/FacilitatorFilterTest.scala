package org.constellation.primitives

import cats.effect.{ContextShift, IO}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.TipData
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema.{CheckpointCacheMetadata, Height}
import org.constellation.domain.schema.Id
import org.constellation.checkpoint.CheckpointService
import org.constellation.{ConstellationExecutionContext, DAO, Fixtures, ProcessingConfig, TestHelpers}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FunSpecLike, Matchers}

class FacilitatorFilterTest
    extends FunSpecLike
    with ArgumentMatchersSugar
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val dao: DAO = mock[DAO]
  dao.id shouldReturn Fixtures.id
  dao.processingConfig shouldReturn ProcessingConfig()

  val checkpointServiceMock: CheckpointService[IO] = mock[CheckpointService[IO]]
  checkpointServiceMock.lookup(*) shouldReturn IO.pure { Some(CheckpointCacheMetadata(null, 0, Some(Height(1, 78)))) }
  dao.checkpointService shouldReturn checkpointServiceMock
  val concurrentTipService: ConcurrentTipService[IO] = mock[ConcurrentTipService[IO]]
  dao.concurrentTipService shouldReturn concurrentTipService

  val calculationContext: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  val facilitatorFilter = new FacilitatorFilter[IO](calculationContext, dao)

  describe("filter facilitators") {
    it("should return 2 facilitators") {
      dao.concurrentTipService.getMinTipHeight(*) shouldReturnF 2
      val peers = TestHelpers.prepareFacilitators(5).toList
      peers.get(0).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node0"), 3L)
      peers.get(1).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node1"), 3L)
      peers.get(2).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node2"), 2L)
      peers.get(3).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node3"), 4L)
      peers.get(4).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node4"), 4L)

      val facilitators = facilitatorFilter.filterPeers(peers.toMap, 2).unsafeRunSync()

      facilitators.size shouldBe 2
    }

    it("should return 1 facilitator") {
      dao.concurrentTipService.getMinTipHeight(*) shouldReturnF 1
      val peers = TestHelpers.prepareFacilitators(5).toList
      peers.get(0).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node0"), 4L)
      peers.get(1).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node1"), 4L)
      peers.get(2).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node2"), 2L)
      peers.get(3).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node3"), 5L)
      peers.get(4).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node4"), 6L)

      val facilitators = facilitatorFilter.filterPeers(peers.toMap, 2).unsafeRunSync()

      facilitators.size shouldBe 1
    }

    it("should return 0 facilitators") {
      dao.concurrentTipService.getMinTipHeight(*) shouldReturnF 2
      val peers = TestHelpers.prepareFacilitators(5).toList
      peers.get(0).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node0"), 5L)
      peers.get(1).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node1"), 6L)
      peers.get(2).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node2"), 7L)
      peers.get(3).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node3"), 8L)
      peers.get(4).get._2.client.getNonBlockingF[IO, (Id, Long)](*, *, *)(*)(*, *, *) shouldReturnF (Id("node4"), 9L)

      val facilitators = facilitatorFilter.filterPeers(peers.toMap, 2).unsafeRunSync()

      facilitators.size shouldBe 0
    }
  }
}
