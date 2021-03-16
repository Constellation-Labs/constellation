package org.constellation.gossip.state

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.mapref.MapRef
import org.constellation.concurrency.MapRefUtils
import org.constellation.util.Metrics

class GossipMessagePathTracker[F[_]: Concurrent, A](metrics: Metrics) {

  private val paths: MapRef[F, String, Option[GossipMessageState]] = MapRefUtils.ofConcurrentHashMap()

  def start(message: GossipMessage[A]): F[Unit] =
    paths(message.path.id).set(GossipMessageState.Pending.some) >> updateMetric

  def success(pathId: String): F[Unit] =
    paths(pathId).modify { maybeState =>
      (maybeState.map(_ => GossipMessageState.Success), ())
    }

  def remove(pathId: String): F[Unit] =
    paths(pathId).modify { _ =>
      (none[GossipMessageState], ())
    } >> updateMetric

  def getRoundState(pathId: String): F[Option[GossipMessageState]] =
    paths(pathId).get

  def isSuccess(message: GossipMessage[A]): F[Boolean] =
    getRoundState(message.path.id).map(_.contains(GossipMessageState.Success))

  private def updateMetric(): F[Unit] =
    paths.keys.flatMap { keys =>
      metrics.updateMetricAsync("gossip_pathTrackingMapSize", keys.size)
    }

}
