package org.constellation.gossip.bisect

import cats.data.OptionT
import cats.effect.Async
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

class Bisect[F[_]: Async] {
  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def runA[A](
    predicate: A => F[Boolean],
    initSeq: Seq[A]
  ): F[Option[A]] = {
    def go(seq: Seq[A], acc: OptionT[F, A]): OptionT[F, A] = seq match {
      case Seq() => acc
      case _ =>
        val (left, right) = seq.splitAt(seq.length / 2)
        OptionT.fromOption[F](right.headOption).flatMap { head =>
          OptionT
            .liftF(predicate(head).handleErrorWith { e =>
              logger.error(e)(s"Predicate evaluation error on $head") >> false.pure[F]
            })
            .ifM(
              go(right.tail, acc),
              go(left, OptionT.some[F](head))
            )
        }
    }

    go(initSeq, OptionT.none[F, A]).value
  }
}

object Bisect {
  def apply[F[_]: Async]: Bisect[F] = new Bisect[F]
}
