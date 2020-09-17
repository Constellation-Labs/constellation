package org.constellation.domain.p2p

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.p2p.PeerHealthCheck.{PeerAvailable, PeerHealthCheckStatus, PeerUnresponsive}
import org.constellation.{PeerMetadata, ResourceInfo}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.infrastructure.p2p.client.{ClusterClientInterpreter, MetricsClientInterpreter}
import org.constellation.p2p.{Cluster, MajorityHeight, PeerData}
import org.constellation.schema.Id
import org.constellation.util.Metrics
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

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
  var apiClient: ClientInterpreter[IO] = _
  var metrics: Metrics = _

  val peer1 = PeerData(
    PeerMetadata("1.2.3.4", 9000, Id("node1"), resourceInfo = mock[ResourceInfo]),
    NonEmptyList.one(MajorityHeight.genesis),
    Seq.empty
  )

  val peer2 = PeerData(
    PeerMetadata("2.3.4.5", 9000, Id("node2"), resourceInfo = mock[ResourceInfo]),
    NonEmptyList.one(MajorityHeight.genesis),
    Seq.empty
  )

  before {
    apiClient = mock[ClientInterpreter[IO]]
    cluster = mock[Cluster[IO]]
    metrics = mock[Metrics]
    peerHealthCheck = PeerHealthCheck(cluster, apiClient, metrics)
    cluster.removePeer(*) shouldReturnF Unit
    cluster.markOfflinePeer(*) shouldReturnF Unit
    cluster.broadcastOfflineNodeState(*) shouldReturnF Unit
    metrics.updateMetricAsync[IO](*, any[String])(*) shouldReturnF Unit
  }

  "check" - {
    "should not remove peers if all are available" in {
      apiClient.metrics shouldReturn mock[MetricsClientInterpreter[IO]]
      apiClient.metrics.checkHealth() shouldReturn Kleisli.apply[IO, PeerClientMetadata, Unit] { _ =>
        IO.unit
      }
      apiClient.cluster shouldReturn mock[ClusterClientInterpreter[IO]]
      apiClient.cluster.checkPeerResponsiveness(*) shouldReturn Kleisli
        .apply[IO, PeerClientMetadata, PeerHealthCheckStatus] { _ =>
          IO.pure(PeerAvailable(0L))
        }

      cluster.broadcast(*, *, *) shouldReturnF Map.empty

      cluster.getPeerInfo shouldReturnF Map(Id("node1") -> peer1, Id("node2") -> peer2)

      peerHealthCheck.check().unsafeRunSync

      cluster.removePeer(*).wasNever(called)
    }

    "should mark peer as offline if peer is unhealthy" in {
      cluster.getPeerInfo shouldReturnF Map(Id("node1") -> peer1, Id("node2") -> peer2)
      apiClient.metrics shouldReturn mock[MetricsClientInterpreter[IO]]
      apiClient.metrics.checkHealth() shouldReturn Kleisli.apply[IO, PeerClientMetadata, Unit] { pm =>
        if (pm.id == Id("node1")) {
          IO.raiseError(new Throwable("error"))
        } else IO.unit
      }
      apiClient.cluster shouldReturn mock[ClusterClientInterpreter[IO]]
      apiClient.cluster.checkPeerResponsiveness(*) shouldReturn Kleisli
        .apply[IO, PeerClientMetadata, PeerHealthCheckStatus] { _ =>
          IO.pure(PeerUnresponsive(0L, 1))
        }
      cluster.broadcast(*, *, *) shouldReturnF Map.empty

      peerHealthCheck.check().unsafeRunSync

      cluster.markOfflinePeer(*).was(called)
    }

    "should broadcast offline state if peer is unhealthy" in {
      cluster.getPeerInfo shouldReturnF Map(Id("node1") -> peer1, Id("node2") -> peer2)
      apiClient.metrics shouldReturn mock[MetricsClientInterpreter[IO]]
      apiClient.metrics.checkHealth() shouldReturn Kleisli.apply[IO, PeerClientMetadata, Unit] { pm =>
        if (pm.id == Id("node1")) {
          IO.raiseError(new Throwable("error"))
        } else IO.unit
      }
      apiClient.cluster shouldReturn mock[ClusterClientInterpreter[IO]]
      apiClient.cluster.checkPeerResponsiveness(*) shouldReturn Kleisli
        .apply[IO, PeerClientMetadata, PeerHealthCheckStatus] { _ =>
          IO.pure(PeerUnresponsive(0L, 1))
        }
      cluster.broadcast(*, *, *) shouldReturnF Map.empty

      peerHealthCheck.check().unsafeRunSync

      cluster.broadcastOfflineNodeState(*).was(called)
    }

    "should broadcast offline state if peer returned error" in {
      cluster.getPeerInfo shouldReturnF Map(Id("node1") -> peer1, Id("node2") -> peer2)
      apiClient.metrics shouldReturn mock[MetricsClientInterpreter[IO]]
      apiClient.metrics.checkHealth() shouldReturn Kleisli.apply[IO, PeerClientMetadata, Unit] { pm =>
        if (pm.id == Id("node1")) {
          IO.raiseError(new Throwable("error"))
        } else IO.unit
      }
      apiClient.cluster shouldReturn mock[ClusterClientInterpreter[IO]]
      apiClient.cluster.checkPeerResponsiveness(*) shouldReturn Kleisli
        .apply[IO, PeerClientMetadata, PeerHealthCheckStatus] { _ =>
          IO.pure(PeerUnresponsive(0L, 1))
        }
      cluster.broadcast(*, *, *) shouldReturnF Map.empty

      peerHealthCheck.check().unsafeRunSync

      cluster.broadcastOfflineNodeState(*).was(called)
    }
  }
}
