package org.constellation

import java.security.PrivateKey

import cats.Functor
import cats.implicits._
import cats.kernel.Monoid
import constellation.{base64, signData}
import org.constellation.primitives.Schema._
import org.constellation.util.{POW, POWSignHelp, ProductHash, Signed}
import constellation._

import scala.collection.concurrent.TrieMap

/**
  * Created by Wyatt on 5/19/18.
  */
trait Cell[A]

/**
  * Monad of cellular execution context, formed by cellular complex of monads forming an ephemeral dag under poset topology sorting.
  * The configuration space, i.e. poincare complex is carried functorally by the cellular complex.
  */
object Cell {
  implicit val cellFunctor: Functor[Cell] {def map[A, B](fa: Cell[A])(f: A => B): Cell[B]} = new Functor[Cell] {
    override def map[A, B](fa: Cell[A])(f: (A) => B): Cell[B] = fa match {
//      case SingularHomology(sheaf) => SingularHomology(f(sheaf))
      case SingularHomology(sheaf) => SingularHomology(sheaf)
      case Homotopy(a, next) => Homotopy(a, f(next))
    }

  }

  def findCommonSubBundles(bundles: Seq[Bundle] = Seq[Bundle]()) = {
    //TODO reshuffle subbindles to make the most efficient site. Could return a full
    val results: Map[Bundle, Int] = bundles.combinations(2).toSeq.flatMap{
      case Seq(l,r) =>
        val sub = l.extractSubBundlesMinSize()
        val subr = r.extractSubBundlesMinSize()
        sub.intersect(subr).toSeq.map {
          b => b -> 1
        }
    }.groupBy(_._1).map{
      case (x,y) => x -> y.size
    }

    val debug = results.map{case (x,y) => (x.short, x.extractTX.size, y)}.toSeq.sortBy{_._2}.reverse.slice(0, 10)
    println(s"bundle common  : ${results.size} $debug")
    results
  }

  val validLedger: TrieMap[String, Long] = TrieMap()
  val memPoolLedger: TrieMap[String, Long] = TrieMap()
  val validSyncPendingUTXO: TrieMap[String, Long] = TrieMap()
  @volatile var memPoolTX: Set[TX] = Set()

  /**
    * Buildup
    */
  val coAlgebra: Sheaf => Cell[Sheaf] = {//ever transaction has to tell us the sender's state
    //  case handleSync => //return a valid sheaf of the sync data the person needed, this is the else below
    case sheaf: Sheaf =>
//      val germ = sheaf.germ.get
//      val txs: Set[TX] = germ.extractTX
//      val newTX = txs.exists{t => !memPoolTX.contains(t)}
//      if (newTX){
//        val commonSubBundles: Map[Bundle, Int] = findCommonSubBundles(Seq(germ))//for every bundle, how many connections to others
//      }
      SingularHomology(sheaf)
  }

  /**
    * Teardown
    */
  val algebra: Cell[Sheaf] => Sheaf = {
    case SingularHomology(sheaf) => sheaf
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
case class Homotopy[A](sheaf: Sheaf, bundle: A) extends Cell[A]


/**
  * Wrapper for maintaining metadata about manifold topology. Useful for combining logic contained in product operators
  */
case class Sheaf(germ: Option[Bundle] = None) extends ProductHash with Monoid[Sheaf] {//TODO turn into monad so we can flatmap collection
  //TODO call the method that invokes minhash/combine 'section' https://arxiv.org/pdf/0907.0995.pdf
  def empty = Sheaf()
  def combine(x: Sheaf, y: Sheaf = this) = Sheaf(None)
}

//TODO define dank bundle as the most optimal site formed given stae and new sheaf.