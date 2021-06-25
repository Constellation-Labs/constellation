package org.constellation.consensus

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.Config
import org.constellation.ConfigUtil
import org.constellation.consensus.ConsensusManager.{ConsensusError, ConsensusStartError}
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.cluster.NodeStorageAlgebra
import org.constellation.domain.redownload.RedownloadStorageAlgebra
import org.constellation.domain.snapshot.SnapshotStorageAlgebra
import org.constellation.schema.NodeState
import org.constellation.util.PeriodicIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ConsensusScheduler(
  config: Config,
  consensusManager: ConsensusManager[IO],
  nodeStorage: NodeStorageAlgebra[IO],
  checkpointStorage: CheckpointStorageAlgebra[IO],
  snapshotStorage: SnapshotStorageAlgebra[IO],
  redownloadStorage: RedownloadStorageAlgebra[IO],
  unboundedExecutionContext: ExecutionContext
) extends PeriodicIO("ConsensusScheduler", unboundedExecutionContext) {

  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")
  val snapshotHeightDelayInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightDelayInterval")
  val distanceFromMajority: Int = ConfigUtil.constellation.getInt("snapshot.distanceFromMajority")

  val crossTalkConsensus: IO[Unit] = consensusManager.startOwnConsensus().void.handleErrorWith {
    case error: ConsensusStartError => IO(logger.debug(error.getMessage))
    case error: ConsensusError      => IO(logger.debug(error.getMessage))
    case unexpected                 => IO(logger.error(unexpected.getMessage))
  }

  val skip: IO[Unit] = IO(logger.debug("Start consensus skipped"))

  override def trigger(): IO[Unit] =
    for {
      canStartOwnConsensus <- nodeStorage.getNodeState.map(NodeState.canStartOwnConsensus)
      distanceFromMajorityNotExceeded <- isDistanceFromMajorityNotExceeded
      minTipDistanceNotExceeded <- isMinTipDistanceNotExceeded
      _ <- if (canStartOwnConsensus && distanceFromMajorityNotExceeded && minTipDistanceNotExceeded) crossTalkConsensus
      else skip
    } yield ()

  def isDistanceFromMajorityNotExceeded: IO[Boolean] =
    for {
      latestMajorityHeight <- redownloadStorage.getLatestMajorityHeight
      nextHeightInterval <- snapshotStorage.getLastSnapshotHeight.map(_ + snapshotHeightInterval)
    } yield nextHeightInterval <= latestMajorityHeight + distanceFromMajority

  def isMinTipDistanceNotExceeded: IO[Boolean] =
    for {
      latestMajorityHeight <- redownloadStorage.getLatestMajorityHeight
      minTipHeight <- checkpointStorage.getMinTipHeight
    } yield minTipHeight <= latestMajorityHeight + snapshotHeightDelayInterval + distanceFromMajority

  schedule(ConfigUtil.getDurationFromConfig("constellation.consensus.start-own-interval", 10 seconds, config))
}
