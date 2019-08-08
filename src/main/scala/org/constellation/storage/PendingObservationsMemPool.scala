package org.constellation.storage

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.primitives.Observation
import org.constellation.primitives.concurrency.SingleRef

class PendingObservationsMemPool[F[_]: Concurrent]() extends PendingMemPool[F, Observation] {

  private val obsRef: SingleRef[F, Map[String, Observation]] =
    SingleRef[F, Map[String, Observation]](Map.empty)

  def put(key: String, value: Observation): F[Observation] =
    obsRef.modify(exs => (exs + (key -> value), value))

  def update(key: String, fn: Observation => Observation): F[Unit] =
    obsRef.update { exs =>
      exs.get(key).map(fn).map(t => exs ++ List(key -> t)).getOrElse(exs)
    }

  def lookup(key: String): F[Option[Observation]] =
    obsRef.get.map(_.find(_._2.hash == key).map(_._2))

  def contains(key: String): F[Boolean] =
    obsRef.get.map(_.exists(_._2.hash == key))

  // TODO: Rethink - use queue
  def pull(minCount: Int): F[Option[List[Observation]]] =
    obsRef.modify { exs =>
      if (exs.size < minCount) {
        (exs, none[List[Observation]])
      } else {
        val (left, right) = exs.toList.splitAt(minCount)
        (right.toMap, left.map(_._2).some)
      }
    }

  def size(): F[Long] =
    obsRef.get.map(_.size.toLong)

}
