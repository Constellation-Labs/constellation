package org.constellation.storage

import cats.implicits._
import cats.effect.Concurrent
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.storage.algebra.LookupAlgebra

abstract class PendingMemPool[F[_]: Concurrent, K, V]() extends LookupAlgebra[F, K, V] {

  val ref: SingleRef[F, Map[K, V]] =
    SingleRef[F, Map[K, V]](Map.empty)

  def pull(maxCount: Int): F[Option[List[V]]]

  def put(key: K, value: V): F[V] =
    ref.modify(txs => (txs + (key -> value), value))

  def update(key: K, fn: V => V): F[Option[V]] =
    ref.modify { as =>
      as.get(key) match {
        case None => (as, None)
        case Some(value) =>
          val update = fn(value)
          (as + (key -> update), Some(update))
      }
    }

  def update(key: K, fn: V => V, empty: => V): F[V] =
    ref.modify { as =>
      as.get(key) match {
        case None => (as + (key -> empty), empty)
        case Some(value) =>
          val update = fn(value)
          (as + (key -> update), update)
      }
    }

  def remove(key: K): F[Unit] =
    ref.modify { m =>
      (m - key, m - key)
    }.void

  def remove(key: Set[K]): F[Unit] =
    ref.modify { m =>
      (m -- key, m -- key)
    }

  def lookup(key: K): F[Option[V]] = ref.get.map(_.get(key))

  def contains(key: K): F[Boolean] =
    ref.get.map(_.contains(key))

  def size(): F[Long] =
    ref.get.map(_.size.toLong)

}
