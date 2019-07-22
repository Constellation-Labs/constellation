package org.constellation.datastore

import cats.effect.IO
import cats.implicits._
import org.constellation.consensus.Snapshot
import org.constellation.util.PeriodicIO
import org.constellation.{ConstellationContextShift, DAO}
import scala.concurrent.duration._

class SnapshotTrigger(periodSeconds: Int = 5)(implicit dao: DAO) extends PeriodicIO("SnapshotTrigger") {

  override def trigger(): IO[Unit] =
    IO.fromFuture(IO(Snapshot.triggerSnapshot(executionNumber.get())))(ConstellationContextShift.global).void

  schedule(periodSeconds seconds)
}
