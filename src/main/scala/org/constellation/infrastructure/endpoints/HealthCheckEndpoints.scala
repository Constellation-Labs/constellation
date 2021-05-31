package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.syntax._
import org.constellation.domain.healthcheck.HealthCheckConsensus.{
  FetchPeerHealthStatus,
  HealthConsensusCommand,
  MissingProposalHealthStatus,
  PingHealthStatus,
  SendConsensusHealthStatus
}
import org.constellation.domain.healthcheck.HealthCheckConsensus.SendConsensusHealthStatus._
import org.constellation.domain.healthcheck.HealthCheckConsensusManagerBase.EitherCodec._
import org.constellation.domain.healthcheck.HealthCheckType.{MissingProposalHealthCheck, PingHealthCheck}
import org.constellation.domain.healthcheck.ping.PingHealthCheckConsensusManager
import org.constellation.domain.healthcheck.proposal.MissingProposalHealthCheckConsensusManager
import org.http4s.dsl.Http4sDsl
import org.http4s.HttpRoutes
import org.http4s.circe._

class HealthCheckEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {
  Slf4jLogger.getLogger[F]

  def peerHealthCheckEndpoints(
    pingHealthCheckConsensusManager: PingHealthCheckConsensusManager[F],
    missingProposalHealthCheckConsensusManager: MissingProposalHealthCheckConsensusManager[F]
  ) =
    processConsensusCommand(pingHealthCheckConsensusManager, missingProposalHealthCheckConsensusManager) <+>
      getRoundProposal(pingHealthCheckConsensusManager, missingProposalHealthCheckConsensusManager) <+>
      getClusterState(pingHealthCheckConsensusManager)

  private def processConsensusCommand(
    pingHealthCheckConsensusManager: PingHealthCheckConsensusManager[F],
    missingProposalHealthCheckConsensusManager: MissingProposalHealthCheckConsensusManager[F]
  ): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "health-check" / "consensus" =>
      for {
        cmd <- req.decodeJson[HealthConsensusCommand]
        response <- cmd match {
          case SendConsensusHealthStatus(consensusHealthStatus, asProxyForId) => {
            asProxyForId match {
              case Some(proxyToPeer) =>
                consensusHealthStatus match {
                  case a: PingHealthStatus =>
                    pingHealthCheckConsensusManager.proxySendHealthProposal(a, proxyToPeer)
                  case a: MissingProposalHealthStatus =>
                    missingProposalHealthCheckConsensusManager.proxySendHealthProposal(a, proxyToPeer)
                }
              case None =>
                consensusHealthStatus match {
                  case a: PingHealthStatus =>
                    pingHealthCheckConsensusManager.handleConsensusHealthProposal(a)
                  case a: MissingProposalHealthStatus =>
                    missingProposalHealthCheckConsensusManager.handleConsensusHealthProposal(a)
                }
            }
          }.flatMap(result => Ok(result.asJson))
        }
      } yield response
  }

  private def getRoundProposal(
    pingHealthCheckConsensusManager: PingHealthCheckConsensusManager[F],
    missingProposalHealthCheckConsensusManager: MissingProposalHealthCheckConsensusManager[F]
  ): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case req @ GET -> Root / "health-check" / "consensus" =>
        for {
          fetchPeerHealthStatus <- req.decodeJson[FetchPeerHealthStatus]
          response <- fetchPeerHealthStatus.asProxyForId match {
            case Some(proxyToPeer) =>
              fetchPeerHealthStatus.healthCheckType match {
                case PingHealthCheck =>
                  pingHealthCheckConsensusManager
                    .proxyGetHealthStatusForRound(
                      fetchPeerHealthStatus.roundIds,
                      proxyToPeer,
                      fetchPeerHealthStatus.originId
                    )
                    .flatMap(result => Ok(result.asJson))
                case MissingProposalHealthCheck =>
                  missingProposalHealthCheckConsensusManager
                    .proxyGetHealthStatusForRound(
                      fetchPeerHealthStatus.roundIds,
                      proxyToPeer,
                      fetchPeerHealthStatus.originId
                    )
                    .flatMap(result => Ok(result.asJson))
              }
            case None =>
              fetchPeerHealthStatus.healthCheckType match {
                case PingHealthCheck =>
                  pingHealthCheckConsensusManager
                    .getHealthStatusForRound(fetchPeerHealthStatus.roundIds, fetchPeerHealthStatus.originId)
                    .flatMap(result => Ok(result.asJson))
                case MissingProposalHealthCheck =>
                  missingProposalHealthCheckConsensusManager
                    .getHealthStatusForRound(fetchPeerHealthStatus.roundIds, fetchPeerHealthStatus.originId)
                    .flatMap(result => Ok(result.asJson))
              }
          }
        } yield response
    }

  private def getClusterState(pingHealthCheckConsensusManager: PingHealthCheckConsensusManager[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "health-check" / "cluster-state" =>
        pingHealthCheckConsensusManager
          .getClusterState()
          .map(_.asJson)
          .flatMap(Ok(_))
    }
}

object HealthCheckEndpoints {

  def peerHealthCheckEndpoints[F[_]: Concurrent](
    pingHealthCheckConsensusManager: PingHealthCheckConsensusManager[F],
    missingProposalHealthCheckConsensusManager: MissingProposalHealthCheckConsensusManager[F]
  ): HttpRoutes[F] =
    new HealthCheckEndpoints[F]
      .peerHealthCheckEndpoints(pingHealthCheckConsensusManager, missingProposalHealthCheckConsensusManager)
}
