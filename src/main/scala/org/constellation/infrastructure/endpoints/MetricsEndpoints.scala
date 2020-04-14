package org.constellation.infrastructure.endpoints

import java.io.{StringWriter, Writer}

import cats.effect.Concurrent
import cats.implicits._
import io.circe.generic.auto._
import io.circe.syntax._
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import org.constellation.primitives.Schema.MetricsResult
import org.constellation.util.Metrics
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class MetricsEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def endpoints(metrics: Metrics) =
    healthEndpoint() <+>
      metricsEndpoint(metrics) <+>
      micrometerMetricsEndpoint(metrics)

  private def healthEndpoint(): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "health" => Ok()
    }

  private def metricsEndpoint(metrics: Metrics): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "metrics" => Ok(MetricsResult(metrics.getMetrics).asJson)
    }

  private def micrometerMetricsEndpoint(metrics: Metrics): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "micrometer-metrics" =>
        F.delay {
          val writer: Writer = new StringWriter()
          TextFormat.write004(writer, metrics.collectorRegistry.metricFamilySamples())
          writer.toString
        }.flatMap(Ok(_))
    }
}

object MetricsEndpoints {

  def publicEndpoints[F[_]: Concurrent](metrics: Metrics): HttpRoutes[F] =
    new MetricsEndpoints[F]().endpoints(metrics)

  def peerEndpoints[F[_]: Concurrent](metrics: Metrics): HttpRoutes[F] =
    new MetricsEndpoints[F]().endpoints(metrics)

  def ownerEndpoints[F[_]: Concurrent](metrics: Metrics): HttpRoutes[F] =
    new MetricsEndpoints[F]().endpoints(metrics)
}
