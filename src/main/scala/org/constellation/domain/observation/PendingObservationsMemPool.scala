package org.constellation.domain.observation

import cats.effect.Concurrent
import cats.syntax.all._
import org.constellation.schema.v2.observation.Observation
import org.constellation.storage.PendingMemPool

class PendingObservationsMemPool[F[_]: Concurrent]() extends PendingMemPool[F, String, Observation] {

  // TODO: Rethink - use queue
  def pull(maxCount: Int): F[Option[List[Observation]]] =
    ref.modify { obs =>
      if (obs.size < 1) {
        (obs, none[List[Observation]])
      } else {
        val (left, right) = obs.toList.splitAt(maxCount)
        (right.toMap, left.map(_._2).some)
      }
    }

}
