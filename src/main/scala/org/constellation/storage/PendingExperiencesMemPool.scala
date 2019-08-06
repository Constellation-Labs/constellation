package org.constellation.storage

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.primitives.Experience
import org.constellation.primitives.concurrency.SingleRef

class PendingExperiencesMemPool[F[_]: Concurrent]() extends PendingMemPool[F, Experience] {

  private val exRef: SingleRef[F, Map[String, Experience]] =
    SingleRef[F, Map[String, Experience]](Map.empty)

  def put(key: String, value: Experience): F[Experience] =
    exRef.modify(exs => (exs + (key -> value), value))

  def update(key: String, fn: Experience => Experience): F[Unit] =
    exRef.update { exs =>
      exs.get(key).map(fn).map(t => exs ++ List(key -> t)).getOrElse(exs)
    }

  def lookup(key: String): F[Option[Experience]] =
    exRef.get.map(_.find(_._2.hash == key).map(_._2))

  def contains(key: String): F[Boolean] =
    exRef.get.map(_.exists(_._2.hash == key))

  // TODO: Rethink - use queue
  def pull(minCount: Int): F[Option[List[Experience]]] =
    exRef.modify { exs =>
      if (exs.size < minCount) {
        (exs, none[List[Experience]])
      } else {
        val (left, right) = exs.toList.splitAt(minCount)
        (right.toMap, left.map(_._2).some)
      }
    }

  def size(): F[Long] =
    exRef.get.map(_.size.toLong)

}
