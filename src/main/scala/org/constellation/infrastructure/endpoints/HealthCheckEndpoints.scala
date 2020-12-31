package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.syntax._
import org.constellation.domain.healthcheck.HealthCheckConsensus.{
  FetchPeerHealthStatus,
  HealthConsensusCommand,
  NotifyAboutMissedConsensus,
  SendPerceivedHealthStatus
}
import org.constellation.domain.healthcheck.HealthCheckConsensusManager
import org.constellation.domain.healthcheck.HealthCheckConsensusManager.SendProposalError
import org.constellation.domain.healthcheck.HealthCheckConsensusManager.EitherCodec._
import org.http4s.dsl.Http4sDsl
import org.http4s.HttpRoutes
import org.http4s.circe._

class HealthCheckEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {
  Slf4jLogger.getLogger[F]

  def peerHealthCheckEndpoints(healthCheckConsensusManager: HealthCheckConsensusManager[F]) =
    processConsensusCommand(healthCheckConsensusManager) <+>
      getRoundProposal(healthCheckConsensusManager) <+>
      getClusterState(healthCheckConsensusManager)

  private def processConsensusCommand(
    healthCheckConsensusManager: HealthCheckConsensusManager[F]
  ): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "health-check" / "consensus" =>
      for {
        cmd <- req.decodeJson[HealthConsensusCommand]
        response <- cmd match {
          case SendPerceivedHealthStatus(consensusHealthStatus, asProxyForId) => {
            asProxyForId match {
              case Some(proxyToPeer) =>
                healthCheckConsensusManager.proxySendHealthProposal(consensusHealthStatus, proxyToPeer)
              case None =>
                healthCheckConsensusManager.handleConsensusHealthProposal(consensusHealthStatus)
            }
          }.flatMap(result => Ok(result.asJson))

          case a @ NotifyAboutMissedConsensus(_, _, _) =>
            F.start(healthCheckConsensusManager.handleNotificationAboutMissedConsensus(a))
              .map(_ => ()) >>=
              (result => Ok(result.asJson)) // I think we may need a separate endpoint here, as we are responding with different type, Unit here, Either above
        }
      } yield response
  }

  private def getRoundProposal(healthCheckConsensusManager: HealthCheckConsensusManager[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case req @ GET -> Root / "health-check" / "consensus" =>
        for {
          fetchPeerHealtStatus <- req.decodeJson[FetchPeerHealthStatus]
          result <- fetchPeerHealtStatus.asProxyForId match {
            case Some(proxyToPeer) =>
              healthCheckConsensusManager
                .proxyGetHealthStatusForRound(fetchPeerHealtStatus.roundIds, proxyToPeer, fetchPeerHealtStatus.originId)
            case None =>
              healthCheckConsensusManager
                .getHealthStatusForRound(fetchPeerHealtStatus.roundIds, fetchPeerHealtStatus.originId)
          }
          response <- Ok(result.asJson)
        } yield response
    }

  private def getClusterState(healthCheckConsensusManager: HealthCheckConsensusManager[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "health-check" / "cluster-state" =>
        healthCheckConsensusManager
          .getClusterState()
          .map(_.asJson)
          .flatMap(Ok(_))
    }
}

object HealthCheckEndpoints {

  def peerHealthCheckEndpoints[F[_]: Concurrent](
    healthCheckConsensusManager: HealthCheckConsensusManager[F]
  ): HttpRoutes[F] =
    new HealthCheckEndpoints[F].peerHealthCheckEndpoints(healthCheckConsensusManager)
}
