package org.constellation.consensus

import org.constellation.consensus.Functor.{Algebra, Coalgebra}

/**
  * Created by Wyatt on 5/19/18.
  */
trait Functor[F[_]] {//TODO Make Bundle extend this or https://typelevel.org/cats/typeclasses/applicative.html
  def map[A, B](fa: F[A])(ab: A => B): F[B]
  def morphism[F[_]: Functor, A, B](a: A)(f: Algebra[F, B], g: Coalgebra[F, A]): B
}

object Functor {
  type Algebra[F[_], A] = F[A] => A
  type AlgebraM[M[_], F[_], A] = F[A] => M[A]

  type Coalgebra[F[_], A] = A => F[A]
  type CoalgebraM[M[_], F[_], A] = A => M[F[A]]
}