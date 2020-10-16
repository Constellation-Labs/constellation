package org.constellation.infrastructure.p2p

import cats.effect.IO
import com.typesafe.config.Config
import org.constellation.{ConfigUtil, ConstellationExecutionContext}
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.util.PeriodicIO

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration._

class PeerHealthCheckWatcher(
  config: Config,
  peerHealthCheck: PeerHealthCheck[IO],
  unboundedHealthExecutionContext: ExecutionContext
) extends PeriodicIO("PeerHealthCheckWatcher", unboundedHealthExecutionContext: ExecutionContext) {

  override def trigger(): IO[Unit] = peerHealthCheck.check()

  schedule(30 seconds, ConfigUtil.getDurationFromConfig("constellation.health-check.p2p-interval", 10 seconds, config))
}

object PeerHealthCheckWatcher {

  def apply(config: Config, peerHealthCheck: PeerHealthCheck[IO], unboundedHealthExecutionContext: ExecutionContext) =
    new PeerHealthCheckWatcher(config, peerHealthCheck, unboundedHealthExecutionContext)
}
