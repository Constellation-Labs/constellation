package org.constellation.storage

import java.net.SocketTimeoutException

import cats.implicits._
import cats.effect.{ContextShift, IO, Timer}
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.primitives.Schema.{Id, NodeState, NodeType}
import org.constellation.primitives.Schema
import org.constellation.util.{APIClient, HealthChecker, HostPort}
import org.constellation.{ConstellationExecutionContext, DAO, PeerMetadata, ProcessingConfig}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.Future

class SnapshotBroadcastServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val timer: Timer[IO] = IO.timer(ConstellationExecutionContext.unbounded)

  var dao: DAO = _
  val healthChecker = mock[HealthChecker[IO]]
  var snapshotBroadcastService: SnapshotBroadcastService[IO] = _

  before {
    dao = mockDAO
    dao.processingConfig shouldReturn ProcessingConfig(recentSnapshotNumber = 3)
    dao.cluster shouldReturn mock[Cluster[IO]]
    healthChecker.checkClusterConsistency(*) shouldReturn IO.pure[Option[List[RecentSnapshot]]](None)
    snapshotBroadcastService = new SnapshotBroadcastService[IO](
      healthChecker,
      dao.cluster,
      dao
    )
  }

  "broadcastSnapshot" - {

    "should broadcastSnapshots " in {
      val readyFacilitators: Map[Schema.Id, PeerData] = Map(prepareFacilitator("a"), prepareFacilitator("b"))
      dao.readyPeers(NodeType.Full) shouldReturnF readyFacilitators
      dao.processingConfig shouldReturn ProcessingConfig(maxInvalidSnapshotRate = 20)
      dao.cluster.getNodeState shouldReturnF NodeState.Ready

      readyFacilitators(Id("a")).client
        .postNonBlockingIO[SnapshotVerification](*, *, *, *)(*, *) shouldReturn IO.fromFuture(IO {
        Future.successful(
          SnapshotVerification(
            VerificationStatus.SnapshotCorrect
          )
        )
      })

      readyFacilitators(Id("b")).client
        .postNonBlockingIO[SnapshotVerification](*, *, *, *)(*, *) shouldReturn IO.fromFuture(IO {
        Future.failed(
          new SocketTimeoutException("timeout")
        )
      })

      val response = snapshotBroadcastService.broadcastSnapshot("snap1", 2)
      response.unsafeRunSync()

      healthChecker.checkClusterConsistency(*).wasNever(called)
    }
  }
  "shouldRunClusterCheck" - {

    "should return true when minimum invalid response were reached" in {
      snapshotBroadcastService.shouldRunClusterCheck(
        List(
          SnapshotVerification(VerificationStatus.SnapshotCorrect).some,
          SnapshotVerification(VerificationStatus.SnapshotInvalid).some,
          SnapshotVerification(VerificationStatus.SnapshotInvalid).some
        )
      ) shouldBe true
    }
    "should return false when minimum invalid response were not reached" in {
      snapshotBroadcastService.shouldRunClusterCheck(
        List(
          SnapshotVerification(VerificationStatus.SnapshotCorrect).some,
          SnapshotVerification(VerificationStatus.SnapshotInvalid).some
        )
      ) shouldBe false
    }
    "should return false when minimum invalid response can't be determined" in {
      snapshotBroadcastService.shouldRunClusterCheck(
        List(
          None,
          None,
          SnapshotVerification(VerificationStatus.SnapshotInvalid).some
        )
      ) shouldBe false
    }
  }

  "updateRecentSnapshots" - {

    "should return only recent snapshots in reversed order" in {

      (1 to 4).toList
        .traverse(i => snapshotBroadcastService.updateRecentSnapshots(i.toString, 0))
        .unsafeRunSync()

      snapshotBroadcastService.getRecentSnapshots
        .unsafeRunSync()
        .map(_.hash) shouldBe List("4", "3", "2")
    }
  }

  private def mockDAO: DAO = mock[DAO]

  private def prepareFacilitator(id: String): (Schema.Id, PeerData) = {

    val facilitatorId = Schema.Id(id)
    val peerData: PeerData = mock[PeerData]
    peerData.peerMetadata shouldReturn mock[PeerMetadata]
    peerData.peerMetadata.id shouldReturn facilitatorId
    peerData.notification shouldReturn Seq()
    peerData.client shouldReturn mock[APIClient]
    peerData.client.hostPortForLogging shouldReturn HostPort(s"http://$id", 9000)
    facilitatorId -> peerData
  }
}
