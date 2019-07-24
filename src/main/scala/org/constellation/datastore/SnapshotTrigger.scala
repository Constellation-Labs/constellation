package org.constellation.datastore

import cats.effect.IO
import org.constellation.DAO
import org.constellation.primitives.Schema.NodeState
import org.constellation.util.{Metrics, PeriodicIO}

import scala.concurrent.duration._

class SnapshotTrigger(periodSeconds: Int = 5)(implicit dao: DAO) extends PeriodicIO("SnapshotTrigger") {

  override def trigger(): IO[Unit] =
    if (executionNumber.get() % dao.processingConfig.snapshotInterval == 0 && dao.nodeState == NodeState.Ready) {
      for {
        startTime <- IO(System.currentTimeMillis())
        snapshotResult <- dao.snapshotService.attemptSnapshot().value
        elapsed <- IO(System.currentTimeMillis() - startTime)
        _ = logger.debug(s"Attempt snapshot took: $elapsed millis")
        _ <- snapshotResult match {
          case Left(err) =>
            IO(logger.debug(s"Snapshot attempt error: $err"))
              .flatMap(_ => dao.metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + Metrics.failure))
          case Right(_) => dao.metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + Metrics.success)
        }
      } yield ()
    } else {
      IO.unit
    }

  schedule(periodSeconds seconds)
}
