package org.constellation.gossip

import cats.Monad
import cats.implicits._

package object bisect {

  def bisectA[F[_]: Monad, A](predicate: A => F[Boolean])(seq: IndexedSeq[A]): F[Option[A]] = {
    val half = Math.floor(seq.length / 2).toInt
    val (left, right) = seq.splitAt(half)

    predicate(left.last)
      .ifA(left.pure[F], right.pure[F])
      .flatMap { partition =>
        if (partition.size == 1) {
          partition.headOption.filterA(predicate)
        } else {
          bisectA(predicate)(partition)
        }
      }
  }
}
