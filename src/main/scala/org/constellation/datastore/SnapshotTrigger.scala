package org.constellation.datastore

import cats.effect.IO
import cats.implicits._
import org.constellation.{ConfigUtil, DAO}
import org.constellation.p2p.Cluster
import org.constellation.util.{Metrics, PeriodicIO}
import org.constellation.util.Logging._

import scala.concurrent.duration._

class SnapshotTrigger(periodSeconds: Int = 5)(implicit dao: DAO, cluster: Cluster[IO])
    extends PeriodicIO("SnapshotTrigger") {

  val snapshotInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotInterval")

  private val preconditions = cluster.isNodeReady.map {
    _ && executionNumber.get() % snapshotInterval == 0
  }

  private def triggerSnapshot(): IO[Unit] =
    preconditions.ifM(
      for {
        _ <- cluster.isNodeReady.ifM(IO.unit, IO.raiseError(new Throwable("Node is not ready")))
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
      } yield (),
      IO.unit
    )

  override def trigger(): IO[Unit] = logThread(triggerSnapshot(), "triggerSnapshot", logger)

  schedule(periodSeconds seconds)
}
