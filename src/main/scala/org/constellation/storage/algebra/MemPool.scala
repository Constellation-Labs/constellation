package org.constellation.storage.algebra

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.primitives.concurrency.SingleRef

class MemPool[F[_]: Concurrent, K, V]() extends LookupAlgebra[F, K, V] {

  private val txRef: SingleRef[F, Map[K, V]] =
    SingleRef[F, Map[K, V]](Map())

  def put(key: K, value: V): F[V] =
    txRef.modify(txs => (txs + (key -> value), value))

  def remove(key: K): F[Unit] =
    txRef.update(txs => txs - key)

  def remove(keys: List[K]): F[Unit] =
    txRef.update(txs => txs -- keys)

  def update(key: K, fn: V => V): F[Option[V]] =
    txRef.modify { txs =>
      txs.get(key) match {
        case None => (txs, None)
        case Some(value) =>
          val update = fn(value)
          (txs + (key -> update), Some(update))
      }
    }

  def lookup(key: K): F[Option[V]] =
    txRef.getUnsafe.map(_.get(key))

  def contains(key: K): F[Boolean] =
    txRef.getUnsafe.map(_.contains(key))

  def size(): F[Long] =
    txRef.getUnsafe.map(_.size.toLong)

}
