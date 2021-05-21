package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.syntax._
import org.constellation.checkpoint.CheckpointService
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class SoeEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def publicEndpoints(checkpointService: CheckpointService[F]) = getSoeEndpoint(checkpointService)

  def peerEndpoints(checkpointService: CheckpointService[F]) = getSoeEndpoint(checkpointService)

  private def getSoeEndpoint(checkpointService: CheckpointService[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "soe" / hash => checkpointService.lookupSoe(hash).map(_.asJson).flatMap(Ok(_))
  }

}

object SoeEndpoints {

  def publicEndpoints[F[_]: Concurrent](checkpointService: CheckpointService[F]): HttpRoutes[F] =
    new SoeEndpoints[F]().publicEndpoints(checkpointService)

  def peerEndpoints[F[_]: Concurrent](checkpointService: CheckpointService[F]): HttpRoutes[F] =
    new SoeEndpoints[F]().peerEndpoints(checkpointService)
}
