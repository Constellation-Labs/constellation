package org.constellation

import cats.Functor
import cats.implicits._
import cats.kernel.Monoid

/**
  * Created by Wyatt on 5/19/18.
  */
trait Cell[A]

/**
  * Monad of cellular execution context, formed by cellular complex of monads forming an ephemeral dag under poset topology sorting.
  * The configuration space, i.e. poincare complex is carried functorally by the cellular complex.
  * Entropic flow, is the use of entropy as a measure for a hausdorff clustering which seeks to find the optimal covering
  * of a lipschitz function.
  */
object Cell {
  implicit val cellFunctor: Functor[Cell] {def map[A, B](fa: Cell[A])(f: A => B): Cell[B]} = new Functor[Cell] {
    override def map[A, B](fa: Cell[A])(f: (A) => B): Cell[B] = fa match {
      case Bundle(sheaf) => Bundle(sheaf)
      case Fiber(a, next) => Fiber(f(a), next)
    }
  }

  /**
    * Buildup
    */
  val coAlgebra: Sheaf => Cell[Sheaf] = s => if (s.depth > 0) fiber(s.copy(depth = 0), s) else bundle()

  /**
    * Teardown
    */
  val algebra: Cell[Sheaf] => Sheaf = {
    case Bundle(result) => result
    case Fiber(acc, next) => acc.combine(next)
  }

  /**
    * Constructor to improve type inference
    *
    * @param result
    * @tparam A
    * @return
    */
  def bundle[A](result: Sheaf = Sheaf(None)): Cell[A] = Bundle(result)

  /**
    * Constructor to improve type inference
    *
    * @param a
    * @param next
    * @tparam A
    * @return
    */
  def fiber[A](a: A, next: Sheaf): Cell[A] = Fiber(a, next)

  /**
    *
    * @param f
    * @param g
    * @tparam F
    * @tparam A
    * @tparam B
    * @return
    */
  def hylo[F[_] : Functor, A, B](f: F[B] => B)(g: A => F[A]): A => B = a => f(g(a) map hylo(f)(g))

  /**
    * basically just a lift, see Streaming: https://patternsinfp.wordpress.com/2017/10/04/metamorphisms/
    *
    * @param f
    * @param g
    * @tparam A
    * @tparam B
    * @return
    */
  def meta[A, B](g: Cell[B] => B)(f: A => Cell[A]): Cell[A] => Cell[B] = a => a map hylo(g)(f)

  /**
    *
    * @param sheaf
    * @return
    */
  def ioF(sheaf: Sheaf): Sheaf = hylo(algebra)(coAlgebra).apply(sheaf)

  /**
    *
    * @param sheaf
    * @return
    */
  def lift(sheaf: Cell[Sheaf]): Cell[Sheaf] = meta(algebra)(coAlgebra)(sheaf)
}

/**
  * Bundles are manifolds. They have relative value with value determined by how many other manifolds are
  * 'stitched' together or share an edge path through this manifold. There is a transitive correlation between dimension (nesting depth)
  * and value. In the limit, higher dimensional manifolds have more memetic influence. Higher dimensional manifolds
  * are also very expensive, allowing us to prevent spam attacks like a proof of work step.
  */
case class Bundle[A](fibers: Sheaf) extends Cell[A]

/**
  *
  * @param bundle
  * @param germ
  * @tparam A
  */
case class Fiber[A](bundle: A, germ: Sheaf) extends Cell[A]

/**
  * Wrapper for maintaining metadata about manifold topology. Useful for combining logic contained in product operators
  */
case class Sheaf(data: Option[Int] = None, var depth: Int = -11) extends Monoid[Sheaf] {
  //TODO call the method that invokes minhash/combine 'section' https://arxiv.org/pdf/0907.0995.pdf
  def empty = Sheaf()

  def combine(x: Sheaf, y: Sheaf = this) = Sheaf(None, x.depth)
}

