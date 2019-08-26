package org.constellation.storage

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.primitives.Observation
import org.constellation.primitives.concurrency.SingleRef

class PendingObservationsMemPool[F[_]: Concurrent]() extends PendingMemPool[F, String, Observation] {

  // TODO: Rethink - use queue
  def pull(maxCount: Int): F[Option[List[Observation]]] =
    ref.modify { exs =>
      if (exs.size < 1) {
        (exs, none[List[Observation]])
      } else {
        val (left, right) = exs.toList.splitAt(maxCount)
        (right.toMap, left.map(_._2).some)
      }
    }

}