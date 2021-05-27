package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.Encoder
import io.circe.syntax._
import io.circe.generic.semiauto._
import org.constellation.checkpoint.CheckpointService
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.schema.Id
import org.constellation.schema.checkpoint.TipData
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class TipsEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def publicEndpoints(
    nodeId: Id,
    checkpointStorage: CheckpointStorageAlgebra[F]
  ) =
    getTipsEndpoint(checkpointStorage) <+>
      getHeights(nodeId, checkpointStorage)

  def peerEndpoints(
    nodeId: Id,
    checkpointStorage: CheckpointStorageAlgebra[F]
  ) =
    getTipsEndpoint(checkpointStorage) <+>
      getHeights(nodeId, checkpointStorage)

  implicit val tipDataMapEncoder: Encoder[Map[String, TipData]] = Encoder.encodeMap[String, TipData]
  implicit val idLongEncoder: Encoder[(Id, Long)] = deriveEncoder

  private def getTipsEndpoint(checkpointStorage: CheckpointStorageAlgebra[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "tips" => checkpointStorage.getTips.map(_.asJson).flatMap(Ok(_))
    }

  private def getHeights(
    nodeId: Id,
    checkpointStorage: CheckpointStorageAlgebra[F]
  ): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "heights" =>
        (for {
          tips <- checkpointStorage.getTips
          response = tips.map(_._2)
        } yield response.asJson).flatMap(Ok(_))

      case GET -> Root / "heights" / "min" =>
        checkpointStorage.getMinTipHeight.map((nodeId, _)).map(_.asJson).flatMap(Ok(_))
    }
}

object TipsEndpoints {

  def publicEndpoints[F[_]: Concurrent](
    nodeId: Id,
    checkpointStorage: CheckpointStorageAlgebra[F]
  ): HttpRoutes[F] =
    new TipsEndpoints[F]().peerEndpoints(nodeId, checkpointStorage)

  def peerEndpoints[F[_]: Concurrent](
    nodeId: Id,
    checkpointStorage: CheckpointStorageAlgebra[F]
  ): HttpRoutes[F] =
    new TipsEndpoints[F]().peerEndpoints(nodeId, checkpointStorage)
}
