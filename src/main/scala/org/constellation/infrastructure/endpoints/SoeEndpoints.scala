package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.syntax._
import org.constellation.checkpoint.CheckpointService
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class SoeEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def publicEndpoints(checkpointStorage: CheckpointStorageAlgebra[F]) = getSoeEndpoint(checkpointStorage)

  def peerEndpoints(checkpointStorage: CheckpointStorageAlgebra[F]) = getSoeEndpoint(checkpointStorage)

  private def getSoeEndpoint(checkpointStorage: CheckpointStorageAlgebra[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "soe" / hash =>
      checkpointStorage.getCheckpoint(hash).map(_.map(_.checkpointBlock.soe)).map(_.asJson).flatMap(Ok(_))
  }

}

object SoeEndpoints {

  def publicEndpoints[F[_]: Concurrent](checkpointStorage: CheckpointStorageAlgebra[F]): HttpRoutes[F] =
    new SoeEndpoints[F]().publicEndpoints(checkpointStorage)

  def peerEndpoints[F[_]: Concurrent](checkpointStorage: CheckpointStorageAlgebra[F]): HttpRoutes[F] =
    new SoeEndpoints[F]().peerEndpoints(checkpointStorage)
}
