package org.constellation.domain.cluster

import cats.effect.{Blocker, Clock, Concurrent, ContextShift}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.{SetNodeStatus, SetStateResult}
import org.constellation.schema.{Id, NodeState}
import org.constellation.util.Logging.logThread
import org.constellation.util.Metrics

class BroadcastService[F[_]: Clock: ContextShift](
  clusterStorage: ClusterStorageAlgebra[F],
  nodeStorage: NodeStorageAlgebra[F],
  apiClient: ClientInterpreter[F],
  metrics: Metrics,
  nodeId: Id,
  unboundedBlocker: Blocker
)(implicit F: Concurrent[F]) {

  implicit val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def compareAndSet(expected: Set[NodeState], newState: NodeState, skipBroadcast: Boolean = false): F[SetStateResult] =
    nodeStorage
      .compareAndSet(expected, newState)
      .flatTap(
        res =>
          if (res.isNewSet) metrics.updateMetricAsync[F]("nodeState", newState.toString)
          else F.unit
      )
      .flatTap {
        case SetStateResult(_, true) if !skipBroadcast && NodeState.broadcastStates.contains(newState) =>
          broadcastNodeState(newState)
        case _ => F.unit
      }

  private def broadcastNodeState(nodeState: NodeState, nodeId: Id = nodeId): F[Unit] =
    logThread(
      broadcast(
        PeerResponse.run(apiClient.cluster.setNodeStatus(SetNodeStatus(nodeId, nodeState)), unboundedBlocker),
        Set(nodeId)
      ).flatTap {
        _.filter(_._2.isLeft).toList.traverse {
          case (id, e) => logger.warn(s"Unable to propagate status to node ID: $id")
        }
      }.void,
      "cluster_broadcastNodeState"
    )

  def broadcast[T](
    f: PeerClientMetadata => F[T],
    skip: Set[Id] = Set.empty,
    subset: Set[Id] = Set.empty,
    forceAllPeers: Boolean = false
  ): F[Map[Id, Either[Throwable, T]]] =
    logThread(
      for {
        peerInfo <- if (forceAllPeers) clusterStorage.getNotOfflinePeers else clusterStorage.getJoinedPeers
        selected = if (subset.nonEmpty) {
          peerInfo.filterKeys(subset.contains)
        } else {
          peerInfo.filterKeys(id => !skip.contains(id))
        }
        (keys, values) = selected.toList.unzip
        res <- values
          .map(_.peerMetadata.toPeerClientMetadata)
          .map(f)
          .traverse { fa =>
            fa.map(_.asRight[Throwable]).handleErrorWith(_.asLeft[T].pure[F])
          }
          .map(v => keys.zip(v).toMap)
      } yield res,
      "cluster_broadcast"
    )
}
