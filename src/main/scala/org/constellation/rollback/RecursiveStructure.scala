package org.constellation.rollback

import cats.{Applicative, Eval, Functor, Traverse}
import cats.implicits._
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.observation.Observation
import org.constellation.rollback.AccountBalances.AccountBalances

object RecursiveStructure {

  case class SnapshotOutput(balances: AccountBalances, observations: Seq[Observation])

  object SnapshotOutput {
    import AccountBalances._

    def apply(snapshot: StoredSnapshot): SnapshotOutput = {
      val blocks = snapshot.checkpointCache
        .map(_.checkpointBlock)

      val balances = blocks
        .map(_.transactions)
        .foldRight(Map.empty[String, Long])(
          (txs, acc) => (spend(txs) |+| received(txs)) |+| acc
        )

      val observations = blocks.flatMap(_.observations)

      new SnapshotOutput(balances, observations)
    }
  }

  sealed trait Stack[A] {

    def traverse[F[_]: Applicative, B](f: A => F[B]): F[Stack[B]] =
      this match {
        case Done(result)  => Applicative[F].pure(Done[B](result))
        case More(a, next) => Applicative[F].map(f(a))(More[B](_, next))
      }
  }

  case class Done[A](result: SnapshotOutput) extends Stack[A]
  case class More[A](a: A, prev: SnapshotOutput) extends Stack[A]

  object Stack {
    implicit val stackFunctor: Functor[Stack] = new Functor[Stack] {
      override def map[A, B](sa: Stack[A])(f: A => B): Stack[B] =
        sa match {
          case Done(result)  => Done(result)
          case More(a, next) => More(f(a), next)
        }
    }

    implicit val stackTraverse: Traverse[Stack] = new Traverse[Stack] {

      def traverse[G[_]: Applicative, A, B](
        fa: Stack[A]
      )(f: A => G[B]): G[Stack[B]] =
        fa.traverse(f)

      override def foldLeft[A, B](fa: Stack[A], b: B)(f: (B, A) => B): B = ???

      override def foldRight[A, B](fa: Stack[A], lb: Eval[B])(
        f: (A, Eval[B]) => Eval[B]
      ): Eval[B] = ???
    }
  }

}
