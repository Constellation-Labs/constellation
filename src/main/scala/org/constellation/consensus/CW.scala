package org.constellation.consensus

/**
  * Created by Wyatt on 6/6/18.
  */
import cats.Functor
import cats.implicits._

sealed trait CW[A]
final case class FiberBundle[A](result: Int) extends CW[A]
final case class FiberGerm[A](a: A, next: Int) extends CW[A]

object CW {
  implicit val stackFunctor: Functor[CW] = new Functor[CW] {
    override def map[A, B](sa: CW[A])(f: A => B): CW[B] =
      sa match {
        case FiberBundle(result) => FiberBundle(result)
        case FiberGerm(a, next) => FiberGerm(f(a), next)
      }
  }

  val stackAlgebra: CW[Int] => Int = {
    case FiberBundle(result) => result
    case FiberGerm(acc, next) => acc * next
  }

  val stackCoalgebra: Int => CW[Int] =
    n => if (n > 0) more(n - 1, n) else done()
//
//  val myAlgebra: CW[Sheaf] => Sheaf = {
//    case FiberBundle(result) => result
//    case FiberGerm(acc, next) => acc * next
//  }
//
//  val myCoalgebra: Sheaf => CW[Sheaf] =
//    n => if (n > 0) more(n - 1, n) else done()

  def done[A](result: Int = 1): CW[A] = FiberBundle(result)
  def more[A](a: A, next: Int): CW[A] = FiberGerm(a, next)

  def bundle[A](result: Sheaf = Sheaf(None)): Cell[A] = Bundle(result)
  def fiber[A](a: A, next: Sheaf): Cell[A] = Fiber(a, next)
}

object FML extends App {

  def hylo[F[_] : Functor, A, B](f: F[B] => B)(g: A => F[A]): A => B =
    a => f(g(a) map hylo(f)(g))

  val test = hylo(CW.stackAlgebra)(CW.stackCoalgebra).apply(5)
  println(test)
}