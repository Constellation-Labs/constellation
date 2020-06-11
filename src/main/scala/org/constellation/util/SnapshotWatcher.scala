package org.constellation.util

import cats.effect.IO
import cats.implicits._
import scala.concurrent.duration._

class SnapshotWatcher(healthChecker: HealthChecker[IO]) extends PeriodicIO("SnapshotWatcher") {

  override def trigger(): IO[Unit] =
    healthChecker.checkForStaleTips()

  schedule(1 minute, 5 seconds)

}
