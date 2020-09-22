package org.constellation.infrastructure.p2p

import cats.effect.IO
import com.typesafe.config.Config
import org.constellation.{ConfigUtil, ConstellationExecutionContext}
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.util.PeriodicIO

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class PeerHealthCheckWatcher(config: Config, peerHealthCheck: PeerHealthCheck[IO])
    extends PeriodicIO("PeerHealthCheckWatcher") {

  override val taskPool: ExecutionContextExecutor = ConstellationExecutionContext.peerHealthCheckPool

  override def trigger(): IO[Unit] = peerHealthCheck.check()

  schedule(30 seconds, ConfigUtil.getDurationFromConfig("constellation.health-check.p2p-interval", 10 seconds, config))
}

object PeerHealthCheckWatcher {
  def apply(config: Config, peerHealthCheck: PeerHealthCheck[IO]) = new PeerHealthCheckWatcher(config, peerHealthCheck)
}
