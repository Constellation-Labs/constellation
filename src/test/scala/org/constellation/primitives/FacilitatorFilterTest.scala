package org.constellation.primitives

import cats.data.Kleisli
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.TipData
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema.{CheckpointCacheMetadata, Height}
import org.constellation.checkpoint.CheckpointService
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.client.SnapshotClientInterpreter
import org.constellation.schema.Id
import org.constellation.{ConstellationExecutionContext, DAO, Fixtures, ProcessingConfig, TestHelpers}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class FacilitatorFilterTest
    extends AnyFunSpecLike
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

  val apiClient = mock[ClientInterpreter[IO]]
  val facilitatorFilter = new FacilitatorFilter[IO](apiClient, dao)

  describe("filter facilitators") {
    it("should return 2 facilitators") {
      val peers = TestHelpers.prepareFacilitators(5).toList
      val heights = peers.zip(List(3L, 2L, 4L, 4L, 4L))
      apiClient.snapshot shouldReturn mock[SnapshotClientInterpreter[IO]]
      apiClient.snapshot.getNextSnapshotHeight() shouldReturn Kleisli.apply[IO, PeerClientMetadata, (Id, Long)] { pm =>
        IO.pure(heights.mapFilter {
          case (pd, height) => if (pd._1 == pm.id) Some(pm.id, height) else None
        }.head)
      }

      val facilitators = facilitatorFilter.filterPeers(peers.toMap, 2, TipSoe(Seq.empty, 2L.some)).unsafeRunSync()

      facilitators.size shouldBe 2
    }

    it("should return 1 facilitator") {
      val peers = TestHelpers.prepareFacilitators(5).toList
      peers.zipWithIndex.foreach {
        case (pd, i) => pd._2.peerMetadata.copy(id = Id(s"node$i"))
      }
      val heights = peers.zip(List(4L, 4L, 2L, 5L, 6L))
      apiClient.snapshot shouldReturn mock[SnapshotClientInterpreter[IO]]
      apiClient.snapshot.getNextSnapshotHeight() shouldReturn Kleisli.apply[IO, PeerClientMetadata, (Id, Long)] { pm =>
        IO.pure(heights.mapFilter {
          case (pd, height) => if (pd._1 == pm.id) Some(pm.id, height) else None
        }.head)
      }

      val facilitators = facilitatorFilter.filterPeers(peers.toMap, 2, TipSoe(Seq.empty, 1L.some)).unsafeRunSync()

      facilitators.size shouldBe 1
    }

    it("should return 0 facilitators") {
      val peers = TestHelpers.prepareFacilitators(5).toList
      peers.zipWithIndex.foreach {
        case (pd, i) => pd._2.peerMetadata.copy(id = Id(s"node$i"))
      }
      val heights = peers.zip(List(5L, 6L, 7L, 8L, 9L))
      apiClient.snapshot shouldReturn mock[SnapshotClientInterpreter[IO]]
      apiClient.snapshot.getNextSnapshotHeight() shouldReturn Kleisli.apply[IO, PeerClientMetadata, (Id, Long)] { pm =>
        IO.pure(heights.mapFilter {
          case (pd, height) => if (pd._1 == pm.id) Some(pm.id, height) else None
        }.head)
      }

      val facilitators = facilitatorFilter.filterPeers(peers.toMap, 2, TipSoe(Seq.empty, 2L.some)).unsafeRunSync()

      facilitators.size shouldBe 0
    }
  }
}
