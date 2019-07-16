package org.constellation.util

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.storage.SnapshotBroadcastService

class SnapshotWatcher(snapshotBroadcastService: SnapshotBroadcastService[IO])
    extends PeriodicIO[Unit]("SnapshotWatcher", 60)
    with StrictLogging {

  override def trigger(): IO[Unit] =
    snapshotBroadcastService.runClusterCheck
}
