package org.constellation.infrastructure.endpoints

import java.io.{StringWriter, Writer}

import cats.effect.Concurrent
import cats.syntax.all._
import io.prometheus.client.exporter.common.TextFormat
import io.circe.syntax._
import org.constellation.schema.v2.MetricsResult
import org.constellation.util.Metrics
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

import MetricsResult._

class MetricsEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def publicEndpoints(metrics: Metrics) =
    healthEndpoint() <+>
      metricsEndpoint(metrics) <+>
      micrometerMetricsEndpoint(metrics)

  def peerEndpoints(metrics: Metrics) =
    metricsEndpoint(metrics) <+>
      micrometerMetricsEndpoint(metrics)

  def ownerEndpoints(metrics: Metrics) =
    healthEndpoint() <+>
      metricsEndpoint(metrics) <+>
      micrometerMetricsEndpoint(metrics)

  def peerHealthCheckEndpoints() =
    healthEndpoint()

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
    new MetricsEndpoints[F]().publicEndpoints(metrics)

  def peerEndpoints[F[_]: Concurrent](metrics: Metrics): HttpRoutes[F] =
    new MetricsEndpoints[F]().peerEndpoints(metrics)

  def ownerEndpoints[F[_]: Concurrent](metrics: Metrics): HttpRoutes[F] =
    new MetricsEndpoints[F]().ownerEndpoints(metrics)

  def peerHealthCheckEndpoints[F[_]: Concurrent](): HttpRoutes[F] =
    new MetricsEndpoints[F]().peerHealthCheckEndpoints()
}
