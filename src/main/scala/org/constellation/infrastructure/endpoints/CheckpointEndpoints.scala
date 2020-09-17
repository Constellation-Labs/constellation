package org.constellation.infrastructure.endpoints

import cats.effect.{Concurrent, ContextShift}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.syntax._
import org.constellation.checkpoint.{CheckpointAcceptanceService, CheckpointService}
import org.constellation.consensus.{FinishedCheckpoint, SignatureRequest}
import org.constellation.domain.observation.ObservationEvent
import org.constellation.primitives.Schema.{GenesisObservation, Height}
import org.constellation.storage.SnapshotService
import org.constellation.util.Metrics
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

import ObservationEvent._
import GenesisObservation._
import org.constellation.primitives.Schema.CheckpointCache._
import org.constellation.consensus.SignatureRequest._
import org.constellation.consensus.SignatureResponse._
import FinishedCheckpoint._

class CheckpointEndpoints[F[_]](implicit F: Concurrent[F], C: ContextShift[F]) extends Http4sDsl[F] {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def peerEndpoints(
    genesisObservation: => Option[GenesisObservation],
    checkpointService: CheckpointService[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F],
    metrics: Metrics,
    snapshotService: SnapshotService[F]
  ) =
    genesisEndpoint(genesisObservation) <+>
      getCheckpointEndpoint(checkpointService) <+>
      requestBlockSignatureEndpoint(checkpointAcceptanceService) <+>
      handleFinishedCheckpointEndpoint(metrics, snapshotService, checkpointAcceptanceService)

  private def genesisEndpoint(genesisObservation: => Option[GenesisObservation]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "genesis" => Ok(genesisObservation.asJson)
  }

  private def getCheckpointEndpoint(checkpointService: CheckpointService[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "checkpoint" / hash => checkpointService.fullData(hash).map(_.asJson).flatMap(Ok(_))
  }

  private def requestBlockSignatureEndpoint(
    checkpointAcceptanceService: CheckpointAcceptanceService[F]
  ): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "request" / "signature" =>
      (for {
        sigRequest <- req.decodeJson[SignatureRequest]
        response <- checkpointAcceptanceService.handleSignatureRequest(sigRequest)
      } yield response.asJson).flatMap(Ok(_))
  }

  private def handleFinishedCheckpointEndpoint(
    metrics: Metrics,
    snapshotService: SnapshotService[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F]
  ): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "finished" / "checkpoint" =>
      for {
        finishedCheckpoint <- req.decodeJson[FinishedCheckpoint]
        baseHash = finishedCheckpoint.checkpointCacheData.checkpointBlock.baseHash
        _ <- logger.debug(s"Handle finished checkpoint for cb: $baseHash")
        _ <- metrics.incrementMetricAsync[F]("peerApiRXFinishedCheckpoint")
        accept = checkpointAcceptanceService.acceptWithNodeCheck(finishedCheckpoint)
        response <- snapshotService.getNextHeightInterval.flatMap { nextHeight =>
          (nextHeight, finishedCheckpoint.checkpointCacheData.height) match {
            case (_, None) =>
              logger.warn(s"Missing height when accepting block hash=$baseHash") >> BadRequest()
            case (2, _) =>
              F.start(accept) >> Accepted()
            case (nextHeight, Some(Height(min, _))) if nextHeight > min =>
              logger.debug(
                s"Handle finished checkpoint for cb: $baseHash height condition not met next interval: $nextHeight received: $min"
              ) >> Conflict()
            case (_, _) =>
              F.start(accept) >> Accepted()
          }
        }
      } yield response
  }

}

object CheckpointEndpoints {

  def peerEndpoints[F[_]: Concurrent: ContextShift](
    genesisObservation: => Option[GenesisObservation],
    checkpointService: CheckpointService[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F],
    metrics: Metrics,
    snapshotService: SnapshotService[F]
  ): HttpRoutes[F] =
    new CheckpointEndpoints[F]()
      .peerEndpoints(genesisObservation, checkpointService, checkpointAcceptanceService, metrics, snapshotService)
}
