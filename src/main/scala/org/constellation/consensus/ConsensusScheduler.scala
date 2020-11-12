package org.constellation.consensus

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.Config
import org.constellation.consensus.ConsensusManager.{ConsensusError, ConsensusStartError}
import org.constellation.p2p.Cluster
import org.constellation.schema.v2.NodeState
import org.constellation.util.PeriodicIO
import org.constellation.{ConfigUtil, DAO}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ConsensusScheduler(
  config: Config,
  consensusManager: ConsensusManager[IO],
  cluster: Cluster[IO],
  dao: DAO,
  unboundedExecutionContext: ExecutionContext
) extends PeriodicIO("ConsensusScheduler", unboundedExecutionContext) {

  val crossTalkConsensus: IO[Unit] = consensusManager.startOwnConsensus().void.handleErrorWith {
    case error: ConsensusStartError => IO(logger.debug(error.getMessage))
    case error: ConsensusError      => IO(logger.debug(error.getMessage))
    case unexpected                 => IO(logger.error(unexpected.getMessage))
  }
  val skip: IO[Unit] = IO(logger.debug("Start consensus skipped"))

  override def trigger(): IO[Unit] =
    if (dao.formCheckpoints) {
      cluster.getNodeState
        .map(NodeState.canStartOwnConsensus)
        .ifM(crossTalkConsensus, skip)
    } else skip

  schedule(ConfigUtil.getDurationFromConfig("constellation.consensus.start-own-interval", 10 seconds, config))
}
