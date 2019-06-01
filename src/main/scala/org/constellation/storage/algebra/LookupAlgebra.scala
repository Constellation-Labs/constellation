package org.constellation.storage.algebra

import cats.effect.Sync
import cats.implicits._

trait LookupAlgebra[F[_], K, V] {
  def lookup(key: K): F[Option[V]]

  def contains(key: K): F[Boolean]
}

object Lookup {

  def extendedLookup[F[_]: Sync, K, V](lookups: List[LookupAlgebra[F, K, V]])(hash: K): F[Option[V]] =
        lookups.foldLeft(none[V].pure[F]) {
          case (x, storage) =>
            x.flatMap { o =>
              o.fold(storage.lookup(hash))(b => b.some.pure[F])
            }
        }

  def extendedContains[F[_]: Sync, K, V](lookups: List[LookupAlgebra[F, K, V]])(hash: K): F[Boolean] =
        lookups.foldLeft(false.pure[F]) {
          case (x, storage) =>
            x.flatMap { o =>
              if (o)
                o.pure[F]
              else
                storage.contains(hash)
            }
        }
}