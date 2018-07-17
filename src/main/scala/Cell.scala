package org.constellation

import java.security.PrivateKey

import cats.Functor
import cats.implicits._
import cats.kernel.Monoid
import constellation.{base64, signData}
import org.constellation.primitives.Schema._
import org.constellation.util.{POW, POWSignHelp, ProductHash, Signed}
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.util.ProductHash

import scala.util.{Random, Try}
/**
  * Created by Wyatt on 5/19/18.
  */
trait Cell[A]

/**
  * Functor of cellular execution context, forms cellular complex with poset topology.
  * The configuration space, i.e. poincare complex is carried functorally by the cellular complex.
  */
object Cell {
  implicit val cellFunctor: Functor[Cell] {def map[A, B](fa: Cell[A])(f: A => B): Cell[B]} = new Functor[Cell] {
    override def map[A, B](fa: Cell[A])(f: A => B): Cell[B] = fa match {
      case SingularHomology(sheaf) => SingularHomology(sheaf)
      case Homology(a, next) => Homology(a, f(next))
    }
  }

  /**
    * Buildup
    */
  val coAlgebra: Sheaf => Cell[Sheaf] = {
    case sheaf: Sheaf =>
//      val ancestors = findAncestorsUpToLastResolved(sheaf.germ.bundle.extractParentBundleHash.pbHash)//TODO add optional arg in cell for parentHash

//      if (lookupBundle(sheaf.germ).isEmpty) storeBundle(sheaf.germ)
//
//      if (ancestors.nonEmpty) {
////      TODO this is where we wordpressuld want to do Homology(sheaf, liftF(Sheaf(res))) for bundles of greater depth
//              SingularHomology(sheaf)
//        }
//      }
      SingularHomology(sheaf)
  }

  /**
    * Teardown
    */
  val algebra: Cell[Sheaf] => Sheaf = {
    case SingularHomology(sheaf) => sheaf
    case hom@Homology(kernal, image) => // TODO: kernal.combine(merge)
      kernal
  }

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
    * @param cell
    * @return
    */
  def liftF(cell: Cell[Sheaf]): Cell[Sheaf] = meta(algebra)(coAlgebra)(cell)
}

/**
  * Bundles are manifolds. They have relative value with value determined by how many other manifolds are
  * 'stitched' together or share an edge path through this manifold. There is a transitive correlation between dimension (nesting depth)
  * and value. In the limit, higher dimensional manifolds have more memetic influence. Higher dimensional manifolds
  * are also very expensive, allowing us to prevent spam attacks like a proof of work step.
  *
  * @param sheaf
  * @tparam A
  */
case class SingularHomology[A](sheaf: Sheaf) extends Cell[A]

/**
  *
  * @param sheaf
  * @param bundle
  * @tparam A
  */
case class Homology[A](sheaf: Sheaf, bundle: A) extends Cell[A]
