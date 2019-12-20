package org.constellation.domain.p2p

import cats.effect.{ContextShift, IO, Timer}
import com.softwaremill.sttp.Response
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.PeerMetadata
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.schema.Id
import org.constellation.util.APIClient
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.ExecutionContext

class PeerHealthCheckTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  implicit val logger = Slf4jLogger.getLogger[IO]

  var cluster: Cluster[IO] = _
  var peerHealthCheck: PeerHealthCheck[IO] = _

  val peer1 = PeerData(
    mock[PeerMetadata],
    mock[APIClient],
    Seq.empty
  )

  val peer2 = PeerData(
    mock[PeerMetadata],
    mock[APIClient],
    Seq.empty
  )

  peer1.client.id shouldReturn Id("node1")
  peer2.client.id shouldReturn Id("node2")
  peer1.client.hostName shouldReturn "1.2.3.4"
  peer2.client.hostName shouldReturn "2.3.4.5"

  before {
    cluster = mock[Cluster[IO]]
    peerHealthCheck = PeerHealthCheck(cluster)
    cluster.removePeer(*) shouldReturnF Unit
    cluster.markOfflinePeer(*) shouldReturnF Unit
    cluster.broadcastOfflineNodeState(*) shouldReturnF Unit
  }

  "check" - {
    "should not remove peers if all are available" in {
      cluster.getPeerInfo shouldReturnF Map(Id("node1") -> peer1, Id("node2") -> peer2)
      peer1.client.getStringF[IO](*, *, *)(*)(*) shouldReturnF Response.ok[String]("OK")
      peer2.client.getStringF[IO](*, *, *)(*)(*) shouldReturnF Response.ok[String]("OK")

      peerHealthCheck.check().unsafeRunSync

      cluster.removePeer(*).wasNever(called)
    }

    "should mark peer as offline if peer is unhealthy" in {
      cluster.getPeerInfo shouldReturnF Map(Id("node1") -> peer1, Id("node2") -> peer2)
      peer1.client.getStringF[IO](*, *, *)(*)(*) shouldReturnF Response.ok[String]("ERROR")
      peer2.client.getStringF[IO](*, *, *)(*)(*) shouldReturnF Response.ok[String]("OK")

      peerHealthCheck.check().unsafeRunSync

      cluster.markOfflinePeer(*).was(called)
    }

    "should mark peer as offline if peer returned error" in {
      cluster.getPeerInfo shouldReturnF Map(Id("node1") -> peer1, Id("node2") -> peer2)
      peer1.client.getStringF[IO](*, *, *)(*)(*) shouldReturnF Response.ok[String]("OK")
      peer2.client.getStringF[IO](*, *, *)(*)(*) shouldReturnF Response
        .error[String]("ERROR", 400, "400")

      peerHealthCheck.check().unsafeRunSync

      cluster.markOfflinePeer(*).was(called)
    }

    "should broadcast offline state if peer is unhealthy" in {
      cluster.getPeerInfo shouldReturnF Map(Id("node1") -> peer1, Id("node2") -> peer2)
      peer1.client.getStringF[IO](*, *, *)(*)(*) shouldReturnF Response.ok[String]("ERROR")
      peer2.client.getStringF[IO](*, *, *)(*)(*) shouldReturnF Response.ok[String]("OK")

      peerHealthCheck.check().unsafeRunSync

      cluster.broadcastOfflineNodeState(*).was(called)
    }

    "should broadcast offline state if peer returned error" in {
      cluster.getPeerInfo shouldReturnF Map(Id("node1") -> peer1, Id("node2") -> peer2)
      peer1.client.getStringF[IO](*, *, *)(*)(*) shouldReturnF Response.ok[String]("OK")
      peer2.client.getStringF[IO](*, *, *)(*)(*) shouldReturnF Response
        .error[String]("ERROR", 400, "400")

      peerHealthCheck.check().unsafeRunSync

      cluster.broadcastOfflineNodeState(*).was(called)
    }
  }
}
