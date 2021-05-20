package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.healthcheck.HealthCheckConsensus
import org.constellation.domain.healthcheck.HealthCheckConsensus.{FetchPeerHealthStatus, SendPerceivedHealthStatus}
import org.constellation.domain.healthcheck.HealthCheckConsensusManager.{FetchProposalError, SendProposalError}
import org.constellation.domain.healthcheck.ReconciliationRound.ClusterState
import org.constellation.domain.p2p.client.HealthCheckClientAlgebra
import org.constellation.domain.healthcheck.HealthCheckConsensusManager.EitherCodec._
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.session.SessionTokenService
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.Method.{GET, POST}
import org.http4s.client.Client

import scala.language.reflectiveCalls

class HealthCheckClientInterpreter[F[_]](
  client: Client[F],
  sessionTokenService: SessionTokenService[F]
)(implicit F: Concurrent[F], CS: ContextShift[F])
    extends HealthCheckClientAlgebra[F] {

  def sendPeerHealthStatus(healthStatus: SendPerceivedHealthStatus): PeerResponse[F, Either[SendProposalError, Unit]] =
    PeerResponse("health-check/consensus", POST)(client, sessionTokenService) { (req, c) =>
      c.expect[Either[SendProposalError, Unit]](req.withEntity(healthStatus))
    }

  def notifyAboutMissedConsensus(notification: HealthCheckConsensus.NotifyAboutMissedConsensus): PeerResponse[F, Unit] =
    PeerResponse("health-check/consensus", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(notification))
    }.flatMapF(
      a => if (a) F.unit else F.raiseError(new Throwable(s"Failed to send notification about missed consensus."))
    )

  def fetchPeerHealthStatus(
    fetchPeerHealthStatus: FetchPeerHealthStatus
  ): PeerResponse[F, Either[FetchProposalError, SendPerceivedHealthStatus]] =
    PeerResponse("health-check/consensus", GET)(client, sessionTokenService) { (req, c) =>
      c.expect[Either[FetchProposalError, SendPerceivedHealthStatus]](req.withEntity(fetchPeerHealthStatus))
    }

  def getClusterState(): PeerResponse[F, ClusterState] =
    PeerResponse[F, ClusterState]("health-check/cluster-state")(client, sessionTokenService)
}

object HealthCheckClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): HealthCheckClientInterpreter[F] = new HealthCheckClientInterpreter(client, sessionTokenService)
}
