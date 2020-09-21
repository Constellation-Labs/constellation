package org.constellation.snapshot

import cats.effect.IO
import org.constellation.util.{HealthChecker, PeriodicIO}

import scala.concurrent.duration._

class SnapshotWatcher(healthChecker: HealthChecker[IO]) extends PeriodicIO("SnapshotWatcher") {

  override def trigger(): IO[Unit] =
    healthChecker.checkForStaleTips()

  schedule(1 minute, 5 seconds)

}
