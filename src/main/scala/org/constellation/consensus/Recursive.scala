package org.constellation.consensus
import cats.Functor
import cats.implicits._
import cats.kernel.{Eq, Monoid}
//import org.constellation.consensus.TopologyManager.{algebra, coAlgebra, hylo}

import scala.concurrent.{CanAwait, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Wyatt on 5/19/18.
  */

trait thang


case class Fiber[A](bundle: A, germ: Sheaf) extends Cell[A]

//TODO: Copy param list from Tx()
//case class Germ(data: Option[Int] = None) extends Sheaf

/**
  * Wrapper for maintaining metadata about manifold topology. Useful for combining logic contained in product operators
  */
case class Sheaf(data: Option[Int] = None, var depth: Int = -11) extends Monoid[Sheaf] {

  def empty = Sheaf()
  def combine(x: Sheaf, y: Sheaf = this) = Sheaf(None, x.depth)
}

/**
  * Bundles are simplectic manifolds. They have relative value with value determined by how many other manifolds are
  * 'stitched' together or share an edge path through this manifold. There is a transitive correlation between dimension
  * and value. In the limit, higher dimensional manifolds have more memetic influence. Higher dimensional manifolds
  * are also very expensive, allowing us to prevent spam attacks like a proof of work step.
  */
case class Bundle[A](fibers: Sheaf) extends Cell[A]

/**
  * TODO make this a monoid?
  * Monad of cellular execution context, formed by cellular complex of monads forming an ephemeral dag under poset topology sorting.
  * The configuration space, i.e. poincare complex is carried functorally by the cellular complex.
  * Entropic flow, is the use of entropy as a measure for a hausdorff clustering which seeks to find the optimal covering
  * of a lipschitz function.
  */
trait Cell[A]

object Cell {
 implicit val cellFunctor: Functor[Cell] {
   def map[A, B](fa: Cell[A])
                (f: A => B): Cell[B]
 } = new Functor[Cell] {
    override def map[A, B](fa: Cell[A])(f: (A) => B): Cell[B] = fa match {
      case Bundle(sheaf) => Bundle(sheaf)
      case Fiber(a, next) => Fiber(f(a), next)
    }

   override def lift[A, B](f: A => B): Cell[A] => Cell[B] = fa => map(fa)(f)
  }

  val coAlgebra: Sheaf => Cell[Sheaf] = s => if (s.depth > 0) fiber(s.copy(depth = 0), s) else bundle()
  val algebra: Cell[Sheaf] => Sheaf = {
    case Bundle(result) => result
    case Fiber(acc, next) => acc.combine(next)
  }

  def bundle[A](result: Sheaf = Sheaf(None)): Cell[A] = Bundle(result)
  def fiber[A](a: A, next: Sheaf): Cell[A] = Fiber(a, next)

  def hylo[F[_]: Functor, A, B](f: F[B] => B)(g: A => F[A]): A => B = a => f(g(a) map hylo(f)(g))

  /**
    * https://patternsinfp.wordpress.com/2017/10/04/metamorphisms/ see Streaming
    * @param f
    * @param g
    * @tparam F
    * @tparam A
    * @tparam B
    * @return
    */
  def meta[F[_]: Functor, A, B](f: A => Cell[A])(g: Cell[B] => B): F[A] => F[B] = a => a map hylo(g)(f)//basically just a lift

  def openStream(sheaf: Sheaf) = hylo(algebra)(coAlgebra).apply(sheaf)

}



trait Recursive {
  case class Fix[F[_]](unfix: F[Fix[F]])

  //Type A => F[A] is a Coalgebra.
  def ana[F[_] : Functor, A](g: A => F[A]): A => Fix[F] = a => Fix(g(a) map ana(g))

  // Type F[A] => A is an Algebra.
  def cata[F[_] : Functor, A](f: F[A] => A): Fix[F] => A = fix => f(fix.unfix map cata(f))

//  def algebra[B](f: Functor[B]): B
//  def coAlgebra[B](g: B): Functor[B]
}