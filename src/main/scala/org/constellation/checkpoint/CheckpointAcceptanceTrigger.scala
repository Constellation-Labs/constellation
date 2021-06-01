package org.constellation.checkpoint

import cats.syntax.all._
import cats.effect.IO
import org.constellation.util.PeriodicIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class CheckpointAcceptanceTrigger(checkpointService: CheckpointService[IO], boundedExecutionContext: ExecutionContext) extends PeriodicIO("checkpointAcceptance", boundedExecutionContext) {

  override def trigger(): IO[Unit] = checkpointService.acceptLock.available.map(_ > 0).ifM(
    checkpointService.acceptNextCheckpoint(),
    IO.unit
  )

  schedule(500.milliseconds)
}
