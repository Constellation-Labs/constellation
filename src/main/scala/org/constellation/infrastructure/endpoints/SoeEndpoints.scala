package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.syntax._
import org.constellation.storage.SOEService
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class SoeEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def peerEndpoints(soeService: SOEService[F]) = getSoeEndpoint(soeService)

  private def getSoeEndpoint(soeService: SOEService[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "soe" / hash => soeService.lookup(hash).map(_.asJson).flatMap(Ok(_))
  }

}

object SoeEndpoints {

  def peerEndpoints[F[_]: Concurrent](soeService: SOEService[F]): HttpRoutes[F] =
    new SoeEndpoints[F]().peerEndpoints(soeService)
}
