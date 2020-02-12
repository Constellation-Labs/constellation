package org.constellation.datastore

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.domain.exception.InvalidNodeState
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}
import org.constellation.p2p.{Cluster, SetStateResult}
import org.constellation.primitives.Schema.NodeState
import org.constellation.storage.{HeightIntervalConditionNotMet, SnapshotError, SnapshotIllegalState}
import org.constellation.util.{Metrics, PeriodicIO}
import org.constellation.util.Logging._

import scala.concurrent.duration._

class SnapshotTrigger(periodSeconds: Int = 5)(implicit dao: DAO, cluster: Cluster[IO])
    extends PeriodicIO("SnapshotTrigger") {

  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  private val preconditions = cluster.getNodeState
    .map(NodeState.canCreateSnapshot)
    .map {
      _ && executionNumber.get() % snapshotHeightInterval == 0
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
          case Left(err @ HeightIntervalConditionNotMet) =>
            resetNodeState(stateSet) >>
              IO(logger.warn(s"Snapshot attempt: ${err.message}")) >>
              dao.metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + "_heightIntervalNotMet")
          case Left(err) =>
            handleError(err, stateSet)
          case Right(created) =>
            resetNodeState(stateSet)
              .flatMap(_ => dao.metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + Metrics.success))
              .flatMap(_ => dao.redownloadService.persistOwnSnapshot(created))
        }
      } yield (),
      IO.unit
    )

  def resetNodeState(stateSet: SetStateResult): IO[SetStateResult] =
    cluster.compareAndSet(Set(NodeState.SnapshotCreation), stateSet.oldState, skipBroadcast = true)

  def handleError(err: SnapshotError, stateSet: SetStateResult): IO[Unit] =
    resetNodeState(stateSet) >>
      IO(logger.warn(s"Snapshot attempt error: ${err.message}"))
        .flatMap(_ => dao.metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + Metrics.failure))

  override def trigger(): IO[Unit] = logThread(triggerSnapshot(), "triggerSnapshot", logger)

  schedule(periodSeconds seconds)
}
