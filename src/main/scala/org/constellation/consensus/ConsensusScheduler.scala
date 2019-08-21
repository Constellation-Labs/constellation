package org.constellation.consensus

import cats.effect.IO
import cats.implicits._
import com.typesafe.config.Config
import org.constellation.consensus.ConsensusManager.{ConsensusError, ConsensusStartError}
import org.constellation.p2p.Cluster
import org.constellation.util.PeriodicIO
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

import scala.concurrent.duration._

class ConsensusScheduler(
  config: Config,
  consensusManager: ConsensusManager[IO],
  cluster: Cluster[IO],
  dao: DAO
) extends PeriodicIO("ConsensusScheduler") {

  val edgeConsensus: IO[Unit] = IO
    .fromFuture(IO {
      EdgeProcessor.formCheckpoint(dao.threadSafeMessageMemPool.pull().getOrElse(Seq()))(dao)
    })(IO.contextShift(ConstellationExecutionContext.bounded))
    .void

  val crossTalkConsensus: IO[Unit] = consensusManager.startOwnConsensus().void.handleErrorWith {
    case error: ConsensusStartError =>
      IO(logger.debug(error.getMessage))
    case error: ConsensusError => IO(logger.warn(error.getMessage))
    case unexpected            => IO(logger.warn(unexpected.getMessage))
  }
  val skip: IO[Unit] = IO(logger.debug("Start consensus skipped"))

  override def trigger(): IO[Unit] =
    (dao.formCheckpoints, dao.nodeConfig.isGenesisNode) match {
      case (false, _)    => skip
      case (true, true)  => cluster.isNodeReady.ifM(edgeConsensus, skip)
      case (true, false) => cluster.isNodeReady.ifM(crossTalkConsensus, skip)
    }

  schedule(ConfigUtil.getDurationFromConfig("constellation.consensus.start-own-interval", 10 seconds, config))
}
