package org.constellation.p2p

import cats.data.ValidatedNel
import cats.effect.{Concurrent, Sync, Timer}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.DAO
import org.constellation.primitives.PeerData
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.{Id, NodeState, NodeType}
import org.constellation.primitives.Schema.NodeType.NodeType
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.util.{APIClient, Metrics}

import scala.concurrent.duration._

class Cluster[F[_]: Concurrent: Logger: Timer](metrics: () => Metrics, dao: DAO) {
  private val nodeState: SingleRef[F, NodeState] = SingleRef[F, NodeState](NodeState.PendingDownload)
//  private val peers: SingleRef[F, Map[Id, PeerData]] = SingleRef[F, Map[Id, PeerData]](Map.empty)

  def getNodeState: F[NodeState] = nodeState.get

  def isNodeReady: F[Boolean] = nodeState.get.map(_ == NodeState.Ready)

  def setNodeState(state: NodeState): F[Unit] =
    nodeState.modify(oldState => (state, oldState)).flatMap { oldState =>
      Logger[F].debug(s"Changing node state from $oldState to $state")
    } *> metrics().updateMetricAsync[F]("nodeState", state.toString)

  /*
  def broadcast[T](
    f: APIClient => F[T],
    skip: Set[Id] = Set.empty,
    subset: Set[Id] = Set.empty
  ): F[Map[Id, Either[Throwable, T]]] =
    for {
      peerInfo <- peers.get
      selected = if (subset.nonEmpty) {
        peerInfo.filterKeys(subset.contains)
      } else {
        peerInfo.filterKeys(id => !skip.contains(id))
      }
      (keys, values) = selected.toList.unzip
      res <- values
        .map(_.client)
        .map(f)
        .traverse { fa =>
          fa.map(_.asRight[Throwable]).recoverWith {
            case e => e.asLeft[T].pure[F]
          }
        }
        .map(v => keys.zip(v).toMap)
    } yield res
   */

  // register & deregister

  def join(): F[Unit] = ???

  def leave(gracefulShutdown: => F[Unit]): F[Unit] =
    for {
      _ <- Logger[F].info("Trying to gracefully leave the cluster")
      // _ <- broadcast leave request
      _ <- Timer[F].sleep(dao.processingConfig.leavingStandbyTimeout.seconds) // TODO: use wkoszycki changes for waiting for last n snapshots
      _ <- setNodeState(NodeState.Offline)
      _ <- gracefulShutdown
    } yield ()
}
