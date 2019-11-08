package org.constellation.consensus

import cats.effect.IO
import cats.implicits._
import com.typesafe.config.Config
import org.constellation.consensus.ConsensusManager.{ConsensusError, ConsensusStartError}
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.NodeState
import org.constellation.schema.HashGenerator
import org.constellation.util.PeriodicIO
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

import scala.concurrent.duration._

class ConsensusScheduler(
  config: Config,
  consensusManager: ConsensusManager[IO],
  cluster: Cluster[IO],
  hashGenerator: HashGenerator,
  dao: DAO
) extends PeriodicIO("ConsensusScheduler") {

  val edgeConsensus: IO[Unit] = IO
    .fromFuture(IO {
      EdgeProcessor.formCheckpoint(dao.threadSafeMessageMemPool.pull().getOrElse(Seq()))(dao, hashGenerator)
    })(IO.contextShift(ConstellationExecutionContext.bounded))
    .void

  val crossTalkConsensus: IO[Unit] = consensusManager.startOwnConsensus().void.handleErrorWith {
    case error: ConsensusStartError => IO(logger.debug(error.getMessage))
    case error: ConsensusError      => IO(logger.debug(error.getMessage))
    case unexpected                 => IO(logger.error(unexpected.getMessage))
  }
  val skip: IO[Unit] = IO(logger.debug("Start consensus skipped"))

  override def trigger(): IO[Unit] =
    (dao.formCheckpoints, dao.nodeConfig.isGenesisNode) match {
      case (false, _) => skip
      case (true, true) =>
        cluster.getNodeState
          .map(NodeState.canStartOwnConsensus)
          .ifM(edgeConsensus, skip)
      case (true, false) =>
        cluster.getNodeState
          .map(NodeState.canStartOwnConsensus)
          .ifM(crossTalkConsensus, skip)
    }

  schedule(ConfigUtil.getDurationFromConfig("constellation.consensus.start-own-interval", 10 seconds, config))
}
