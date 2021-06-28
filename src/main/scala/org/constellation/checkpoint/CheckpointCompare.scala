package org.constellation.checkpoint

import cats.effect.IO
import org.constellation.util.PeriodicIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class CheckpointCompare(
  checkpointService: CheckpointService[IO],
  unboundedExecutionContext: ExecutionContext
) extends PeriodicIO("CheckpointCompare", unboundedExecutionContext) {

  override def trigger(): IO[Unit] = checkpointService.compareAcceptedCheckpoints()

  schedule(10.seconds)
}
