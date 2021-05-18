package org.constellation.infrastructure.endpoints

import cats.data.Validated.{Invalid, Valid}
import cats.effect.{Concurrent, ContextShift}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.syntax._
import org.constellation.checkpoint.{CheckpointAcceptanceService, CheckpointService}
import org.constellation.gossip.checkpoint.CheckpointBlockGossipService
import org.constellation.gossip.state.GossipMessage
import org.constellation.gossip.validation.{
  EndOfCycle,
  IncorrectReceiverId,
  IncorrectSenderId,
  MessageValidator,
  PathDoesNotStartAndEndWithOrigin
}
import org.constellation.storage.SnapshotService
import org.constellation.util.Metrics
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response}
import org.constellation.schema.signature.SignatureRequest._
import org.constellation.schema.signature.SignatureResponse._
import org.constellation.schema.checkpoint.{CheckpointBlockPayload, FinishedCheckpoint}
import org.constellation.schema.{GenesisObservation, Height, Id}
import org.constellation.schema.observation.ObservationEvent
import org.constellation.schema.signature.SignatureRequest
import org.constellation.session.Registration.`X-Id`

class CheckpointEndpoints[F[_]](implicit F: Concurrent[F], C: ContextShift[F]) extends Http4sDsl[F] {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def peerEndpoints(
    genesisObservation: => Option[GenesisObservation],
    checkpointService: CheckpointService[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F],
    metrics: Metrics,
    snapshotService: SnapshotService[F],
    checkpointBlockGossipService: CheckpointBlockGossipService[F],
    messageValidator: MessageValidator
  ) =
    genesisEndpoint(genesisObservation) <+>
      getCheckpointEndpoint(checkpointService) <+>
      checkFinishedCheckpointEndpoint(checkpointService) <+>
      requestBlockSignatureEndpoint(checkpointAcceptanceService) <+>
      handleFinishedCheckpointEndpoint(metrics, snapshotService, checkpointAcceptanceService) <+>
      postFinishedCheckpoint(
        checkpointBlockGossipService,
        messageValidator,
        snapshotService,
        checkpointService,
        checkpointAcceptanceService
      )

  private def genesisEndpoint(genesisObservation: => Option[GenesisObservation]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "genesis" => Ok(genesisObservation.asJson)
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

  private[endpoints] def postFinishedCheckpoint(
    checkpointBlockGossipService: CheckpointBlockGossipService[F],
    messageValidator: MessageValidator,
    snapshotService: SnapshotService[F],
    checkpointService: CheckpointService[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F]
  ): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case req @ POST -> Root / "peer" / "checkpoint" / "finished" =>
        for {
          message <- req.as[GossipMessage[CheckpointBlockPayload]]
          payload = message.payload
          senderId = req.headers.get(`X-Id`).map(_.value).map(Id(_)).get

          res <- messageValidator.validateForForward(message, senderId) match {
            case Invalid(EndOfCycle)                                                            => checkpointBlockGossipService.finishCycle(message) >> Ok()
            case Invalid(IncorrectReceiverId(_, _)) | Invalid(PathDoesNotStartAndEndWithOrigin) => BadRequest()
            case Invalid(IncorrectSenderId(_))                                                  => Response[F](status = Unauthorized).pure[F]
            case Invalid(e)                                                                     => logger.error(e)(e.getMessage) >> InternalServerError()
            case Valid(_) =>
              val accept = checkpointAcceptanceService.acceptWithNodeCheck(payload.block.value)
              val processFinishedCheckpointAsync = F.start(
                C.shift >>
                  snapshotService.getNextHeightInterval.flatMap { nextHeight =>
                    (nextHeight, payload.block.value.checkpointCacheData.height) match {
                      case (_, None)                                              => F.unit
                      case (2, _)                                                 => F.start(accept) >> F.unit
                      case (nextHeight, Some(Height(min, _))) if nextHeight > min => F.unit
                      case (_, _)                                                 => F.start(accept) >> F.unit
                    }
                  } >>
                  checkpointBlockGossipService.spread(message)
              )

              checkpointService.gossipCache.put(
                payload.block.value.checkpointCacheData.checkpointBlock.soeHash,
                payload.block.value.checkpointCacheData
              ) >>
                payload.block.validSignature
                  .pure[F]
                  .ifM(
                    processFinishedCheckpointAsync >> Ok(),
                    BadRequest()
                  )
          }
        } yield res
    }

  private def checkFinishedCheckpointEndpoint(checkpointService: CheckpointService[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "checkpoint" / hash / "check" =>
        checkpointService.gossipCache.lookup(hash).map(_.asJson).flatMap(Ok(_))
    }

  def publicEndpoints(checkpointService: CheckpointService[F]) = getCheckpointEndpoint(checkpointService)

  private def getCheckpointEndpoint(checkpointService: CheckpointService[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "checkpoint" / hash => checkpointService.fullData(hash).map(_.asJson).flatMap(Ok(_))
  }

}

object CheckpointEndpoints {

  def publicEndpoints[F[_]: Concurrent: ContextShift](
    checkpointService: CheckpointService[F]
  ): HttpRoutes[F] = new CheckpointEndpoints[F]().publicEndpoints(checkpointService)

  def peerEndpoints[F[_]: Concurrent: ContextShift](
    genesisObservation: => Option[GenesisObservation],
    checkpointService: CheckpointService[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F],
    metrics: Metrics,
    snapshotService: SnapshotService[F],
    checkpointBlockGossipService: CheckpointBlockGossipService[F],
    messageValidator: MessageValidator
  ): HttpRoutes[F] =
    new CheckpointEndpoints[F]()
      .peerEndpoints(
        genesisObservation,
        checkpointService,
        checkpointAcceptanceService,
        metrics,
        snapshotService,
        checkpointBlockGossipService,
        messageValidator
      )
}
