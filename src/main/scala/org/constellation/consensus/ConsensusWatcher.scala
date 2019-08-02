package org.constellation.consensus

import cats.effect.IO
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.constellation.ConfigUtil
import org.constellation.util.PeriodicIO

import scala.concurrent.duration._

class ConsensusWatcher(config: Config, consensusManager: ConsensusManager[IO])
    extends PeriodicIO("ConsensusWatcher")
    with StrictLogging {

  override def trigger(): IO[Unit] =
    consensusManager.cleanUpLongRunningConsensus

  schedule(ConfigUtil.getDurationFromConfig("constellation.consensus.cleanup-interval", 10 seconds, config))

}
