package org.constellation.consensus

import cats.effect.IO
import com.typesafe.config.Config
import org.constellation.ConfigUtil
import org.constellation.util.PeriodicIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ConsensusWatcher(
  config: Config,
  consensusManager: ConsensusManager[IO],
  unboundedExecutionContext: ExecutionContext
) extends PeriodicIO("ConsensusWatcher", unboundedExecutionContext) {

  override def trigger(): IO[Unit] =
    consensusManager.cleanUpLongRunningConsensus

  schedule(ConfigUtil.getDurationFromConfig("constellation.consensus.cleanup-interval", 10 seconds, config))

}
