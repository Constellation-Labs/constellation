package org.constellation.util

import cats.effect.IO
import cats.implicits._
import org.constellation.storage.SnapshotBroadcastService
import scala.concurrent.duration._

class SnapshotWatcher(snapshotBroadcastService: SnapshotBroadcastService[IO]) extends PeriodicIO("SnapshotWatcher") {

  override def trigger(): IO[Unit] =
    snapshotBroadcastService.runClusterCheck

  schedule(3 minute, 10 seconds)

}
