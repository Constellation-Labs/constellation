package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import io.circe.{Decoder, Encoder}
import org.constellation.domain.healthcheck.HealthCheckConsensus.{FetchPeerHealthStatus, SendConsensusHealthStatus}
import org.constellation.domain.healthcheck.HealthCheckConsensusManagerBase.{FetchProposalError, SendProposalError}
import org.constellation.domain.healthcheck.ping.ReconciliationRound.ClusterState
import org.constellation.domain.p2p.client.HealthCheckClientAlgebra
import org.constellation.domain.healthcheck.HealthCheckConsensusManagerBase.EitherCodec._
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

  def sendPeerHealthStatus[A <: SendConsensusHealthStatus[_, _, _]: Encoder](
    healthStatus: A
  ): PeerResponse[F, Either[SendProposalError, Unit]] =
    PeerResponse("health-check/consensus", POST)(client, sessionTokenService) { (req, c) =>
      c.expect[Either[SendProposalError, Unit]](req.withEntity(healthStatus))
    }

  def fetchPeerHealthStatus[A <: SendConsensusHealthStatus[_, _, _]: Decoder](
    fetchPeerHealthStatus: FetchPeerHealthStatus
  ): PeerResponse[F, Either[FetchProposalError, A]] =
    PeerResponse("health-check/consensus", GET)(client, sessionTokenService) { (req, c) =>
      c.expect[Either[FetchProposalError, A]](req.withEntity(fetchPeerHealthStatus))
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
