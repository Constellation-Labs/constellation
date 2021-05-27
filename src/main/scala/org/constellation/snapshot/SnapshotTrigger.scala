package org.constellation.snapshot

import cats.effect.IO
import cats.syntax.all._
import org.constellation.domain.cluster.NodeStorageAlgebra
import org.constellation.domain.exception.InvalidNodeState
import org.constellation.domain.redownload.{RedownloadService, RedownloadStorageAlgebra}
import org.constellation.gossip.snapshot.SnapshotProposalGossipService
import org.constellation.p2p.{Cluster, SetStateResult}
import org.constellation.schema.NodeState
import org.constellation.schema.signature.Signed.signed
import org.constellation.schema.snapshot.{MajorityInfo, SnapshotProposal, SnapshotProposalPayload}
import org.constellation.storage.{
  HeightIntervalConditionNotMet,
  NotEnoughSpace,
  SnapshotError,
  SnapshotIllegalState,
  SnapshotService
}
import org.constellation.util.Logging._
import org.constellation.util.{Metrics, PeriodicIO}
import org.constellation.{ConfigUtil, DAO}

import java.security.KeyPair
import scala.collection.SortedMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class SnapshotTrigger(periodSeconds: Int = 5, unboundedExecutionContext: ExecutionContext)(
  cluster: Cluster[IO],
  nodeStorage: NodeStorageAlgebra[IO],
  snapshotProposalGossipService: SnapshotProposalGossipService[IO],
  metrics: Metrics,
  keyPair: KeyPair,
  redownloadStorage: RedownloadStorageAlgebra[IO],
  snapshotService: SnapshotService[IO]
) extends PeriodicIO("SnapshotTrigger", unboundedExecutionContext) {

  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  private val preconditions = nodeStorage.getNodeState
    .map(NodeState.canCreateSnapshot)
    .map {
      _ && executionNumber.get() % snapshotHeightInterval == 0
    }

  private def triggerSnapshot(): IO[Unit] =
    preconditions.ifM(
      for {
        stateSet <- nodeStorage.compareAndSet(NodeState.validForSnapshotCreation, NodeState.SnapshotCreation)
        _ <- if (!stateSet.isNewSet)
          IO.raiseError(InvalidNodeState(NodeState.validForSnapshotCreation, stateSet.oldState))
        else IO.unit
        startTime <- IO(System.currentTimeMillis())
        snapshotResult <- snapshotService.attemptSnapshot().value
        elapsed <- IO(System.currentTimeMillis() - startTime)
        majorityRange <- redownloadStorage.getMajorityRange
        majorityGapRanges <- redownloadStorage.getMajorityGapRanges
        _ = logger.debug(s"Attempt snapshot took: $elapsed millis")
        _ <- snapshotResult match {
          case Left(NotEnoughSpace) =>
            (IO.shift >> cluster.leave(IO.unit)).start >> handleError(NotEnoughSpace, stateSet)
          case Left(SnapshotIllegalState) =>
            handleError(SnapshotIllegalState, stateSet)
          case Left(err @ HeightIntervalConditionNotMet) =>
            resetNodeState(stateSet) >>
              IO(logger.warn(s"Snapshot attempt: ${err.message}")) >>
              metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + "_heightIntervalNotMet")
          case Left(err) =>
            handleError(err, stateSet)
          case Right(created) =>
            metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + Metrics.success) >>
              redownloadStorage
                .persistCreatedSnapshot(created.height, created.hash, SortedMap(created.publicReputation.toSeq: _*)) >>
              redownloadStorage
                .persistAcceptedSnapshot(created.height, created.hash) >>
              resetNodeState(stateSet) >>
              snapshotProposalGossipService
                .spread(
                  SnapshotProposalPayload(
                    signed(
                      SnapshotProposal(
                        created.hash,
                        created.height,
                        SortedMap(created.publicReputation.toSeq: _*)
                      ),
                      keyPair
                    ),
                    MajorityInfo(
                      majorityRange,
                      majorityGapRanges
                    )
                  )
                )
                .start

        }
      } yield (),
      IO.unit
    )

  def resetNodeState(stateSet: SetStateResult): IO[SetStateResult] =
    nodeStorage.compareAndSet(Set(NodeState.SnapshotCreation), stateSet.oldState)

  def handleError(err: SnapshotError, stateSet: SetStateResult): IO[Unit] = {
    implicit val cs = contextShift

    resetNodeState(stateSet) >>
      IO(logger.warn(s"Snapshot attempt error: ${err.message}")) >>
      metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + Metrics.failure)
  }

  override def trigger(): IO[Unit] = logThread(triggerSnapshot(), "triggerSnapshot", logger)

  schedule(periodSeconds seconds)
}
