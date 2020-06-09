package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.implicits._
import io.circe.syntax._
import org.constellation.domain.observation.ObservationService
import org.constellation.util.Metrics
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.constellation.domain.observation.Observation._

class ObservationEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def peerEndpoints(observationService: ObservationService[F], metrics: Metrics) =
    getObservationByHash(observationService) <+>
      getBatchEndpoint(metrics, observationService)

  private def getObservationByHash(observationService: ObservationService[F]) = HttpRoutes.of[F] {
    case GET -> Root / "observation" / hash =>
      observationService.lookup(hash).map(_.asJson).flatMap(Ok(_))
  }

  private def getBatchEndpoint(metrics: Metrics, observationService: ObservationService[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case req @ POST -> Root / "batch" / "observations" =>
        (for {
          hashes <- req.decodeJson[List[String]]
          _ <- metrics.incrementMetricAsync[F](Metrics.batchObservationsEndpoint)
          obs <- hashes.traverse(hash => observationService.lookup(hash).map((hash, _))).map(_.filter(_._2.isDefined))
        } yield obs.asJson).flatMap(Ok(_))
    }

}

object ObservationEndpoints {

  def peerEndpoints[F[_]: Concurrent](observationService: ObservationService[F], metrics: Metrics): HttpRoutes[F] =
    new ObservationEndpoints[F]().peerEndpoints(observationService, metrics)
}
