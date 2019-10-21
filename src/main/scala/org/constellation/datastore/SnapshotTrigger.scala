package org.constellation.datastore

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.domain.exception.InvalidNodeState
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}
import org.constellation.p2p.{Cluster, SetStateResult}
import org.constellation.primitives.Schema.NodeState
import org.constellation.storage.{SnapshotError, SnapshotIllegalState}
import org.constellation.util.{Metrics, PeriodicIO}
import org.constellation.util.Logging._

import scala.async.internal.StateSet
import scala.concurrent.duration._

class SnapshotTrigger(periodSeconds: Int = 5)(implicit dao: DAO, cluster: Cluster[IO])
    extends PeriodicIO("SnapshotTrigger") {

  val snapshotInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotInterval")

  val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  private val preconditions = cluster.getNodeState
    .map(NodeState.canCreateSnapshot)
    .map {
      _ && executionNumber.get() % snapshotInterval == 0
    }

  private def triggerSnapshot(): IO[Unit] =
    preconditions.ifM(
      for {
        stateSet <- dao.cluster
          .compareAndSet(NodeState.validForSnapshotCreation, NodeState.SnapshotCreation, skipBroadcast = true)
        _ <- if (!stateSet.isNewSet)
          IO.raiseError(InvalidNodeState(NodeState.validForSnapshotCreation, stateSet.oldState))
        else IO.unit
        startTime <- IO(System.currentTimeMillis())
        snapshotResult <- dao.snapshotService.attemptSnapshot().value
        elapsed <- IO(System.currentTimeMillis() - startTime)
        _ = logger.debug(s"Attempt snapshot took: $elapsed millis")
        _ <- snapshotResult match {
          case Left(SnapshotIllegalState) =>
            handleError(SnapshotIllegalState, stateSet)
              .flatMap(_ => contextShift.shift >> dao.snapshotBroadcastService.verifyRecentSnapshots())
          case Left(err) =>
            handleError(err, stateSet)
          case Right(created) =>
            dao.cluster
              .compareAndSet(Set(NodeState.SnapshotCreation), stateSet.oldState, skipBroadcast = true)
              .flatMap(_ => dao.metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + Metrics.success))
              .flatMap(_ => dao.snapshotBroadcastService.broadcastSnapshot(created.hash, created.height))
        }
      } yield (),
      IO.unit
    )

  def handleError(err: SnapshotError, stateSet: SetStateResult) =
    dao.cluster.compareAndSet(Set(NodeState.SnapshotCreation), stateSet.oldState, skipBroadcast = true) >>
      IO(logger.debug(s"Snapshot attempt error: $err"))
        .flatMap(_ => dao.metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + Metrics.failure))

  override def trigger(): IO[Unit] = logThread(triggerSnapshot(), "triggerSnapshot", logger)

  schedule(periodSeconds seconds)
}
