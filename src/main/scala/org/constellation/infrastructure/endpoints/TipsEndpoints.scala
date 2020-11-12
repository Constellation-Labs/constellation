package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.Encoder
import io.circe.syntax._
import io.circe.generic.semiauto._
import org.constellation.checkpoint.CheckpointService
import org.constellation.schema.v2.Id
import org.constellation.schema.v2.checkpoint.TipData
import org.constellation.storage.ConcurrentTipService
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class TipsEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def publicEndpoints(
    nodeId: Id,
    concurrentTipService: ConcurrentTipService[F],
    checkpointService: CheckpointService[F]
  ) =
    getTipsEndpoint(concurrentTipService: ConcurrentTipService[F]) <+>
      getHeights(nodeId, concurrentTipService, checkpointService)

  def peerEndpoints(
    nodeId: Id,
    concurrentTipService: ConcurrentTipService[F],
    checkpointService: CheckpointService[F]
  ) =
    getTipsEndpoint(concurrentTipService: ConcurrentTipService[F]) <+>
      getHeights(nodeId, concurrentTipService, checkpointService)

  implicit val tipDataMapEncoder: Encoder[Map[String, TipData]] = Encoder.encodeMap[String, TipData]
  implicit val idLongEncoder: Encoder[(Id, Long)] = deriveEncoder

  private def getTipsEndpoint(concurrentTipService: ConcurrentTipService[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "tips" => concurrentTipService.toMap.map(_.asJson).flatMap(Ok(_))
    }

  private def getHeights(
    nodeId: Id,
    concurrentTipService: ConcurrentTipService[F],
    checkpointService: CheckpointService[F]
  ): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "heights" =>
        (for {
          tips <- concurrentTipService.toMap
          maybeHeights <- tips.toList.traverse { case (hash, _) => checkpointService.lookup(hash) }
          response = maybeHeights.flatMap(_.flatMap(_.height))
        } yield response.asJson).flatMap(Ok(_))

      case GET -> Root / "heights" / "min" =>
        concurrentTipService.getMinTipHeight(None).map((nodeId, _)).map(_.asJson).flatMap(Ok(_))
    }
}

object TipsEndpoints {

  def publicEndpoints[F[_]: Concurrent](
    nodeId: Id,
    concurrentTipService: ConcurrentTipService[F],
    checkpointService: CheckpointService[F]
  ): HttpRoutes[F] =
    new TipsEndpoints[F]().peerEndpoints(nodeId, concurrentTipService, checkpointService)

  def peerEndpoints[F[_]: Concurrent](
    nodeId: Id,
    concurrentTipService: ConcurrentTipService[F],
    checkpointService: CheckpointService[F]
  ): HttpRoutes[F] =
    new TipsEndpoints[F]().peerEndpoints(nodeId, concurrentTipService, checkpointService)
}
