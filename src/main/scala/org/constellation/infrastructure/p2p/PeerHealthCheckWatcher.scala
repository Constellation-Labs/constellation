package org.constellation.infrastructure.p2p

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.Config
import org.constellation.domain.healthcheck.HealthCheckConsensusManagerBase
import org.constellation.domain.healthcheck.ping.PingHealthCheckConsensusManager
import org.constellation.domain.healthcheck.proposal.MissingProposalHealthCheckConsensusManager
import org.constellation.ConfigUtil
import org.constellation.util.PeriodicIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class PeerHealthCheckWatcher(
  config: Config,
  pingHealthCheckConsensusManager: PingHealthCheckConsensusManager[IO],
  missingProposalHealthCheckConsensusManager: MissingProposalHealthCheckConsensusManager[IO],
  unboundedHealthExecutionContext: ExecutionContext
) extends PeriodicIO("PeerHealthCheckWatcher", unboundedHealthExecutionContext: ExecutionContext) {

  override def trigger(): IO[Unit] = {
    List[HealthCheckConsensusManagerBase[IO, _, _, _, _]](
      pingHealthCheckConsensusManager,
      missingProposalHealthCheckConsensusManager
    ).parTraverse[IO, Unit] { manager =>
      manager.triggerHealthcheckManagement().handleErrorWith { e =>
        manager.logger.warn(e)("Running periodic healthcheck management failed!")
      }
    }
  }.void

  schedule(30 seconds, ConfigUtil.getDurationFromConfig("constellation.health-check.p2p-interval", 5 seconds, config))
}

object PeerHealthCheckWatcher {

  def apply(
    config: Config,
    pingHealthCheckConsensusManager: PingHealthCheckConsensusManager[IO],
    missingProposalHealthCheckConsensusManager: MissingProposalHealthCheckConsensusManager[IO],
    unboundedHealthExecutionContext: ExecutionContext
  ) =
    new PeerHealthCheckWatcher(
      config,
      pingHealthCheckConsensusManager,
      missingProposalHealthCheckConsensusManager,
      unboundedHealthExecutionContext
    )
}
