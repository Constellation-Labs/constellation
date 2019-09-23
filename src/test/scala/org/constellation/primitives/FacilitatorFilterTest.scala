package org.constellation.primitives

import cats.effect.{ContextShift, IO}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.TipData
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema.{CheckpointCacheMetadata, Height, Id}
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

  val calculationContext: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  val facilitatorFilter = new FacilitatorFilter[IO](calculationContext, dao)

  describe("filter facilitators") {
    it("should return 2 facilitators") {
      val ownTips: Map[String, TipData] = Map("id1" -> null, "id2" -> null)
      val peers: Map[Id, PeerData] = TestHelpers.prepareFacilitators(5)
      peers.map(_._2.client).foreach { client =>
        client.getNonBlockingF[IO, List[Height]](*, *, *)(*)(*, *, *) shouldReturn
          List(Height(2, 70), Height(3, 72), Height(1, 45)).pure[IO]
      }

      val facilitators = facilitatorFilter.filterPeers(peers, ownTips, 2).unsafeRunSync()

      facilitators.size shouldBe 2
    }

    it("should return 1 facilitator") {
      val ownTips: Map[String, TipData] = Map("id1" -> null, "id2" -> null)
      val peers: Map[Id, PeerData] = TestHelpers.prepareFacilitators(5)
      peers.slice(0, 5).map(_._2.client).foreach { client =>
        client.getNonBlockingF[IO, List[Height]](*, *, *)(*)(*, *, *) shouldReturn
          List(Height(5, 70), Height(3, 72), Height(4, 45)).pure[IO]
      }
      peers.last._2.client.getNonBlockingF[IO, List[Height]](*, *, *)(*)(*, *, *) shouldReturn
        List(Height(2, 70), Height(3, 72), Height(1, 45)).pure[IO]

      val facilitators = facilitatorFilter.filterPeers(peers, ownTips, 2).unsafeRunSync()

      facilitators.size shouldBe 1
    }

    it("should return 0 facilitators") {
      val ownTips: Map[String, TipData] = Map("id1" -> null, "id2" -> null)
      val peers: Map[Id, PeerData] = TestHelpers.prepareFacilitators(5)
      peers.map(_._2.client).foreach { client =>
        client.getNonBlockingF[IO, List[Height]](*, *, *)(*)(*, *, *) shouldReturn
          List(Height(5, 70), Height(3, 72), Height(4, 45)).pure[IO]
      }

      val facilitators = facilitatorFilter.filterPeers(peers, ownTips, 2).unsafeRunSync()

      facilitators.size shouldBe 0
    }
  }
}
