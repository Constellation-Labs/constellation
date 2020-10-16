package org.constellation.snapshot

import cats.effect.IO
import org.constellation.util.{HealthChecker, PeriodicIO}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class SnapshotWatcher(healthChecker: HealthChecker[IO], unboundedExecutionContext: ExecutionContext)
    extends PeriodicIO("SnapshotWatcher", unboundedExecutionContext) {

  override def trigger(): IO[Unit] =
    healthChecker.checkForStaleTips()

  schedule(1 minute, 5 seconds)

}
