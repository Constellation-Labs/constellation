package org.constellation.checkpoint

import cats.effect.IO
import cats.syntax.all._
import org.constellation.domain.cluster.NodeStorageAlgebra
import org.constellation.schema.NodeState
import org.constellation.util.PeriodicIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class CheckpointAcceptanceTrigger(
  nodeStorage: NodeStorageAlgebra[IO],
  checkpointService: CheckpointService[IO],
  unboundedExecutionContext: ExecutionContext
) extends PeriodicIO("checkpointAcceptance", unboundedExecutionContext) {

  override def trigger(): IO[Unit] =
    nodeStorage.getNodeState
      .map(NodeState.canAcceptCheckpoint)
      .ifM(
        checkpointService.acceptLock.available
          .map(_ > 0)
          .ifM(
            checkpointService.acceptNextCheckpoint(),
            IO.unit
          ),
        IO.unit
      )

  schedule(5.milliseconds)
}

class CheckpointAcceptanceRecalculationTrigger(
  checkpointService: CheckpointService[IO],
  boundedExecutionContext: ExecutionContext
) extends PeriodicIO("checkpointRecalculation", boundedExecutionContext) {

  override def trigger(): IO[Unit] = checkpointService.recalculateQueue()

  schedule(500.millis)
}
