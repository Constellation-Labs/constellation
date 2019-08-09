package org.constellation.storage

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.primitives.Observation
import org.constellation.primitives.concurrency.SingleRef

class PendingObservationsMemPool[F[_]: Concurrent]() extends PendingMemPool[F, String, Observation] {

  // TODO: Rethink - use queue
  def pull(minCount: Int): F[Option[List[Observation]]] =
    ref.modify { exs =>
      if (exs.size < minCount) {
        (exs, none[List[Observation]])
      } else {
        val (left, right) = exs.toList.splitAt(minCount)
        (right.toMap, left.map(_._2).some)
      }
    }

}
