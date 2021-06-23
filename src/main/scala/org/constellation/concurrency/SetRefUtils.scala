package org.constellation.concurrency

import cats.syntax.all._
import cats.effect.Sync
import cats.effect.concurrent.Ref

object SetRefUtils {
  implicit class RefOps[F[_]: Sync, K, V](val ref: Ref[F, Set[K]]) {

    def add(k: K): F[Unit] =
      ref.modify(a => (a + k, ()))

    def remove(k: K): F[Unit] =
      ref.modify(a => (a - k, ()))

    def exists(k: K): F[Boolean] =
      ref.get.map(_.contains(k))
  }
}
