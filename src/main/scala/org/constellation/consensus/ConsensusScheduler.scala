package org.constellation.consensus

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.Config
import org.constellation.consensus.ConsensusManager.{ConsensusError, ConsensusStartError}
import org.constellation.domain.cluster.NodeStorageAlgebra
import org.constellation.p2p.Cluster
import org.constellation.schema.NodeState
import org.constellation.util.PeriodicIO
import org.constellation.{ConfigUtil, DAO}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ConsensusScheduler(
  config: Config,
  consensusManager: ConsensusManager[IO],
  nodeStorage: NodeStorageAlgebra[IO],
  unboundedExecutionContext: ExecutionContext
) extends PeriodicIO("ConsensusScheduler", unboundedExecutionContext) {

  val crossTalkConsensus: IO[Unit] = consensusManager.startOwnConsensus().void.handleErrorWith {
    case error: ConsensusStartError => IO(logger.debug(error.getMessage))
    case error: ConsensusError      => IO(logger.debug(error.getMessage))
    case unexpected                 => IO(logger.error(unexpected.getMessage))
  }
  val skip: IO[Unit] = IO(logger.debug("Start consensus skipped"))

  override def trigger(): IO[Unit] =
    nodeStorage.getNodeState
      .map(NodeState.canStartOwnConsensus)
      .ifM(crossTalkConsensus, skip)

  schedule(ConfigUtil.getDurationFromConfig("constellation.consensus.start-own-interval", 1 seconds, config))
}
