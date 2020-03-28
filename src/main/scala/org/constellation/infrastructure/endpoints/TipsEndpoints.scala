package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.implicits._
import io.circe.generic.auto._
import io.circe.syntax._
import org.constellation.checkpoint.CheckpointService
import org.constellation.primitives.ConcurrentTipService
import org.constellation.schema.Id
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class TipsEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def peerEndpoints(
    nodeId: Id,
    concurrentTipService: ConcurrentTipService[F],
    checkpointService: CheckpointService[F]
  ) =
    getTipsEndpoint(concurrentTipService: ConcurrentTipService[F]) <+>
      getHeights(nodeId, concurrentTipService, checkpointService)

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

  def peerEndpoints[F[_]: Concurrent](
    nodeId: Id,
    concurrentTipService: ConcurrentTipService[F],
    checkpointService: CheckpointService[F]
  ): HttpRoutes[F] =
    new TipsEndpoints[F]().peerEndpoints(nodeId, concurrentTipService, checkpointService)
}
