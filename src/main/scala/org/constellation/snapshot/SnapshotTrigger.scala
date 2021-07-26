package org.constellation.snapshot

import cats.effect.IO
import cats.syntax.all._
import org.constellation.domain.cluster.{BroadcastService, ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.domain.exception.InvalidNodeState
import org.constellation.domain.redownload.{RedownloadService, RedownloadStorageAlgebra}
import org.constellation.gossip.snapshot.SnapshotProposalGossipService
import org.constellation.p2p.{Cluster, SetStateResult}
import org.constellation.schema.NodeState
import org.constellation.schema.signature.Signed.signed
import org.constellation.schema.snapshot.{SnapshotProposal, SnapshotProposalPayload}
import org.constellation.storage.{
  HeightIntervalConditionNotMet,
  NodeNotPartOfL0FacilitatorsPool,
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
  clusterStorage: ClusterStorageAlgebra[IO],
  nodeStorage: NodeStorageAlgebra[IO],
  snapshotProposalGossipService: SnapshotProposalGossipService[IO],
  metrics: Metrics,
  keyPair: KeyPair,
  redownloadStorage: RedownloadStorageAlgebra[IO],
  snapshotService: SnapshotService[IO],
  broadcastService: BroadcastService[IO]
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
        stateSet <- broadcastService.compareAndSet(NodeState.validForSnapshotCreation, NodeState.SnapshotCreation)
        _ <- if (!stateSet.isNewSet)
          IO.raiseError(InvalidNodeState(NodeState.validForSnapshotCreation, stateSet.oldState))
        else IO.unit
        startTime <- IO(System.currentTimeMillis())
        snapshotResult <- snapshotService.attemptSnapshot().value
        elapsed <- IO(System.currentTimeMillis() - startTime)
        filterData <- redownloadStorage.localFilterData
        _ = logger.debug(s"Attempt snapshot took: $elapsed millis")
        _ <- snapshotResult match {
          case Left(NotEnoughSpace) =>
            (IO.shift >> cluster.leave(IO.unit)).start >> handleError(NotEnoughSpace, stateSet)
          case Left(SnapshotIllegalState) =>
            handleError(SnapshotIllegalState, stateSet)
          case Left(err @ NodeNotPartOfL0FacilitatorsPool) =>
            handleError(err, stateSet)
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
              markLeavingPeersAsOffline() >>
              removeOfflinePeers() >>
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
                    filterData
                  )
                )
                .start

        }
      } yield (),
      IO.unit
    )

  def resetNodeState(stateSet: SetStateResult): IO[SetStateResult] =
    broadcastService.compareAndSet(Set(NodeState.SnapshotCreation), stateSet.oldState)

  def handleError(err: SnapshotError, stateSet: SetStateResult): IO[Unit] = {
    implicit val cs = contextShift

    resetNodeState(stateSet) >>
      IO(logger.warn(s"Snapshot attempt error: ${err.message}")) >>
      metrics.incrementMetricAsync[IO](Metrics.snapshotAttempt + Metrics.failure)
  }

  private def markLeavingPeersAsOffline(): IO[Unit] =
    clusterStorage.getPeers
      .map(_.filter {
        case (id, pd) => Set[NodeState](NodeState.Leaving).contains(pd.peerMetadata.nodeState)
      })
      .flatMap {
        _.values.toList.map(_.peerMetadata.id).traverse { p =>
          cluster
            .markOfflinePeer(p)
            .handleErrorWith(err => IO.delay { logger.warn(s"Cannot mark leaving peer as offline: ${err.getMessage}") })
        }
      }
      .void

  private def removeOfflinePeers(): IO[Unit] =
    clusterStorage.getPeers
      .map(_.filter {
        case (id, pd) => NodeState.offlineStates.contains(pd.peerMetadata.nodeState)
      })
      .flatMap {
        _.values.toList.traverse { p =>
          cluster
            .removePeer(p)
            .handleErrorWith(err => IO.delay { logger.warn(s"Cannot remove offline peer: ${err.getMessage}") })
        }
      }
      .void

  override def trigger(): IO[Unit] = logThread(triggerSnapshot(), "triggerSnapshot", logger)

  schedule(periodSeconds seconds)
}
