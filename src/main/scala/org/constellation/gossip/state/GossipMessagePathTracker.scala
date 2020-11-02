package org.constellation.gossip.state

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._

class GossipMessagePathTracker[F[_]: Concurrent, A] {
  private val paths: Ref[F, Map[String, GossipMessageState]] = Ref.unsafe(Map.empty)

  def start(message: GossipMessage[A]): F[Unit] =
    paths.modify { m =>
      (m + (message.path.id -> GossipMessageState.Pending), ())
    }

  def fail(pathId: String): F[Unit] =
    paths.modify { m =>
      val updated = m
        .get(pathId)
        .map { _ =>
          m.updated(pathId, GossipMessageState.PendingAfterFailure)
        }
        .getOrElse(m)

      (updated, ())
    }

  def success(pathId: String): F[Unit] =
    paths.modify { m =>
      val updated = m
        .get(pathId)
        .map { _ =>
          m.updated(pathId, GossipMessageState.Success)
        }
        .getOrElse(m)

      (updated, ())
    }

  def remove(pathId: String): F[Unit] =
    paths.modify { m =>
      val updated = m
        .get(pathId)
        .map { _ =>
          m - pathId
        }
        .getOrElse(m)

      (updated, ())
    }

  def getRoundState(pathId: String): F[Option[GossipMessageState]] =
    paths.modify { m =>
      (m, m.get(pathId))
    }

  def isSuccess(message: GossipMessage[A]): F[Boolean] =
    getRoundState(message.path.id).map(_.contains(GossipMessageState.Success))

}
