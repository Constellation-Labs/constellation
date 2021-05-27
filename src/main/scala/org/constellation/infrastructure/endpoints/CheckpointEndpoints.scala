package org.constellation.infrastructure.endpoints

import cats.data.Validated.{Invalid, Valid}
import cats.effect.{Concurrent, ContextShift}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.syntax._
import org.constellation.checkpoint.CheckpointService
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.genesis.GenesisStorageAlgebra
import org.constellation.gossip.checkpoint.CheckpointBlockGossipService
import org.constellation.gossip.state.GossipMessage
import org.constellation.gossip.validation.{EndOfCycle, IncorrectReceiverId, IncorrectSenderId, MessageValidator, PathDoesNotStartAndEndWithOrigin}
import org.constellation.storage.SnapshotService
import org.constellation.util.Metrics
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response}
import org.constellation.schema.signature.SignatureRequest._
import org.constellation.schema.signature.SignatureResponse._
import org.constellation.schema.checkpoint.CheckpointBlockPayload
import org.constellation.schema.{GenesisObservation, Height, Id}
import org.constellation.schema.observation.ObservationEvent
import org.constellation.schema.signature.SignatureRequest
import org.constellation.session.Registration.`X-Id`

class CheckpointEndpoints[F[_]](implicit F: Concurrent[F], C: ContextShift[F]) extends Http4sDsl[F] {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def peerEndpoints(
                     genesisStorage: GenesisStorageAlgebra[F],
    checkpointService: CheckpointService[F],
    metrics: Metrics,
    snapshotService: SnapshotService[F],
    checkpointBlockGossipService: CheckpointBlockGossipService[F],
    messageValidator: MessageValidator,
    checkpointStorage: CheckpointStorageAlgebra[F]
  ) =
    genesisEndpoint(genesisStorage) <+>
      getCheckpointEndpoint(checkpointStorage) <+>
      requestBlockSignatureEndpoint(checkpointService) <+>
      postFinishedCheckpoint(
        checkpointBlockGossipService,
        messageValidator,
        snapshotService,
        checkpointService,
        checkpointStorage
      )

  private def genesisEndpoint(genesisStorage: GenesisStorageAlgebra[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "genesis" => genesisStorage.getGenesisObservation >>= { go => Ok(go.asJson) }
  }

  private def requestBlockSignatureEndpoint(
    checkpointService: CheckpointService[F]
  ): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "request" / "signature" =>
      (for {
        sigRequest <- req.decodeJson[SignatureRequest]
        response <- checkpointService.handleSignatureRequest(sigRequest)
      } yield response.asJson).flatMap(Ok(_))
  }

  private[endpoints] def postFinishedCheckpoint(
    checkpointBlockGossipService: CheckpointBlockGossipService[F],
    messageValidator: MessageValidator,
    snapshotService: SnapshotService[F],
    checkpointService: CheckpointService[F],
    checkpointStorage: CheckpointStorageAlgebra[F]
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
              val accept = checkpointService.acceptWithNodeCheck(payload.block.value)
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

              payload.block.validSignature
                .pure[F]
                .ifM(
                  checkpointStorage.persistCheckpoint(payload.block.value.checkpointCacheData) >>
                    checkpointStorage.registerUsage(payload.block.value.checkpointCacheData.checkpointBlock.soeHash) >>
                    processFinishedCheckpointAsync >> Ok(),
                  BadRequest()
                )
          }
        } yield res
    }

  def publicEndpoints(checkpointStorage: CheckpointStorageAlgebra[F]) = getCheckpointEndpoint(checkpointStorage)

  private def getCheckpointEndpoint(checkpointStorage: CheckpointStorageAlgebra[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "checkpoint" / soeHash => checkpointStorage.getCheckpoint(soeHash).map(_.asJson).flatMap(Ok(_))
  }

}

object CheckpointEndpoints {

  def publicEndpoints[F[_]: Concurrent: ContextShift](
    checkpointStorage: CheckpointStorageAlgebra[F]
  ): HttpRoutes[F] = new CheckpointEndpoints[F]().publicEndpoints(checkpointStorage)

  def peerEndpoints[F[_]: Concurrent: ContextShift](
    genesisStorage: GenesisStorageAlgebra[F],
    checkpointService: CheckpointService[F],
    metrics: Metrics,
    snapshotService: SnapshotService[F],
    checkpointBlockGossipService: CheckpointBlockGossipService[F],
    messageValidator: MessageValidator,
    checkpointStorage: CheckpointStorageAlgebra[F]
  ): HttpRoutes[F] =
    new CheckpointEndpoints[F]()
      .peerEndpoints(
        genesisStorage,
        checkpointService,
        metrics,
        snapshotService,
        checkpointBlockGossipService,
        messageValidator,
        checkpointStorage
      )
}
