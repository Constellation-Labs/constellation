package org.constellation.util

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.storage.SnapshotBroadcastService
import scala.concurrent.duration._

class SnapshotWatcher(snapshotBroadcastService: SnapshotBroadcastService[IO])
    extends PeriodicIO("SnapshotWatcher")
    with StrictLogging {

  override def trigger(): IO[Unit] = IO.unit // mwadon: Temp disable
//    snapshotBroadcastService.runClusterCheck

  schedule(3 minute, 10 seconds)

}
