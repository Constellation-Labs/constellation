package org.constellation.domain.healthcheck

import cats.effect.Sync
import org.constellation.util.Metrics

class PrefixedHealthCheckMetrics[F[_]: Sync](metrics: Metrics, healthCheckType: HealthCheckType) {

  val prefix = s"healthcheck_${healthCheckType}_"

  def updateMetricAsync(key: String, value: Int): F[Unit] = metrics.updateMetricAsync(prefix + key, value)
  def updateMetricAsync(key: String, value: Long): F[Unit] = metrics.updateMetricAsync(prefix + key, value)
  def updateMetricAsync(key: String, value: Double): F[Unit] = metrics.updateMetricAsync(prefix + key, value)
  def updateMetricAsync(key: String, value: String): F[Unit] = metrics.updateMetricAsync(prefix + key, value)
  def incrementMetricAsync(key: String): F[Unit] = metrics.incrementMetricAsync(prefix + key)

}
