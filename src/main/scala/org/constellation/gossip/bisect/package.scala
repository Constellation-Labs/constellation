package org.constellation.gossip

import cats.Monad
import cats.data.OptionT
import cats.implicits._
package object bisect {

  def bisectA[F[_]: Monad, A](predicate: A => F[Boolean], initSeq: Seq[A]): F[Option[A]] = {
    def go(seq: Seq[A], acc: OptionT[F, A]): OptionT[F, A] = seq match {
      case Seq() => acc
      case _ =>
        val (left, right) = seq.splitAt(seq.length / 2)
        OptionT.fromOption[F](right.headOption).flatMap { head =>
          OptionT
            .liftF(predicate(head))
            .ifM(
              go(right.tail, acc),
              go(left, OptionT.some[F](head))
            )
        }
    }

    go(initSeq, OptionT.none[F, A]).value
  }

}
