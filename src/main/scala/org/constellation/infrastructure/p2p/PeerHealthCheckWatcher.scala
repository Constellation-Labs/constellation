package org.constellation.infrastructure.p2p

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.Config
import org.constellation.domain.healthcheck.HealthCheckConsensusManager
import org.constellation.{ConfigUtil, ConstellationExecutionContext}
import org.constellation.util.PeriodicIO

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration._

class PeerHealthCheckWatcher(
  config: Config,
  healthCheckConsensusManager: HealthCheckConsensusManager[IO],
  unboundedHealthExecutionContext: ExecutionContext
) extends PeriodicIO("PeerHealthCheckWatcher", unboundedHealthExecutionContext: ExecutionContext) {

  override def trigger(): IO[Unit] = healthCheckConsensusManager.triggerHealthcheckManagement()

  schedule(30 seconds, ConfigUtil.getDurationFromConfig("constellation.health-check.p2p-interval", 5 seconds, config))
}

object PeerHealthCheckWatcher {

  def apply(
    config: Config,
    healthCheckConsensusManager: HealthCheckConsensusManager[IO],
    unboundedHealthExecutionContext: ExecutionContext
  ) =
    new PeerHealthCheckWatcher(config, healthCheckConsensusManager, unboundedHealthExecutionContext)
}
