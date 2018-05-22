package org.constellation.consensus
import cats.Functor
import cats.implicits._

/**
  * Created by Wyatt on 5/19/18.
  */

/**
  * TODO Make case class
  */
trait Fiber

/**
  * Bundles are simplectic manifolds. They have relative value with value determined by how many other manifolds are
  * 'stitched' together or share an edge path through this manifold. There is a transitive correlation between dimension
  * and value. In the limit, higher dimensional manifolds have more memetic influence. Higher dimensional manifolds
  * are also very expensive, allowing us to prevent spam attacks like a proof of work step.
  */
case class Bundle(fibers: Recursive*) extends Recursive

/**
  * This is a covering of covers. Conflicting bundles are gathered, consensus is applied, then the result is scattered. Consensus is a gather apply scatter
  * @param bundles pev referenced bundles
  */
case class Block(bundles: Bundle*)

/**
  * Monoad of cellular execution context, formed by cellular complex of monads forming an ephemeral dag under poset topology sorting.
  * The configuration space, i.e. poincare complex is carried functorally by the cellular complex.
  * Entropic flow, is the use of entropy as a measure for a hausdorff clustering which seeks to find the optimal covering
  * of a lipschitz function.
  */
abstract class Cell extends Functor[Recursive]

trait Recursive {

  def algebra[B](f: Functor[B]): B
  def coAlgebra[B](g: B): Functor[B]

  case class Fix[F[_]](unfix: F[Fix[F]])
  //Type A => F[A] is a Coalgebra.
  def ana[F[_] : Functor, A](g: A => F[A]): A => Fix[F] = a => Fix(g(a) map ana(g))

  // Type F[A] => A is an Algebra.
  def cata[F[_] : Functor, A](f: F[A] => A): Fix[F] => A = fix => f(fix.unfix map cata(f))
}