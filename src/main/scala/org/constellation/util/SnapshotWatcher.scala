package org.constellation.util

import cats.effect.IO
import cats.implicits._
import scala.concurrent.duration._

class SnapshotWatcher(healthChecker: HealthChecker[IO]) extends PeriodicIO("SnapshotWatcher") {

  override def trigger(): IO[Unit] =
    healthChecker.checkForStaleTips()

  schedule(3 minute, 10 seconds)

}
