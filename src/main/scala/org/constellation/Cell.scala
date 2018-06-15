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
      case FiberBundle(sheaf) => FiberBundle(f(sheaf))
    }
  }

  def findCommonSubBundles(bundles: Seq[Bundle] = Seq[Bundle]()) = {
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
      //sign stuff, call manager for most recent poset bundle
      //InternalHeartbeat should be able to count as poset bundle
      val germ = sheaf.germ.get
      val txs: Set[TX] = germ.extractTX
      val ids = germ.extractIds
      val commonSubBundles: Map[Bundle, Int] = findCommonSubBundles()//for every bundle, how many connections to others

      val valid = txs.forall(t => t.ledgerValid(validLedger) && t.valid && t.ledgerValid(memPoolLedger))//TODO make these futures and flatmap them
      val newTX = txs.exists{t => !memPoolTX.contains(t)}
//    todo add  if (valid && newTX)
        FiberBundle(sheaf)//pass in commonSubBundles as well so we can make dank bundle if configured, if we need more
    // info, send partially applied FiberBundle to algebra and recursively reduce over meta calls.
  }

  /**
    * Teardown
    */
  val algebra: Cell[Sheaf] => Sheaf = {
    case FiberBundle(result) =>
      if (true){result}//TODO do we need more network info to make this guy
    else result//Otherwise return
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
    * @param sheaf
    * @return
    */
  def liftF(sheaf: Cell[Sheaf]): Cell[Sheaf] = meta(algebra)(coAlgebra)(sheaf)
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
case class FiberBundle[A](sheaf: A) extends Cell[A]

/**
  * Wrapper for maintaining metadata about manifold topology. Useful for combining logic contained in product operators
  */
case class Sheaf(germ: Option[Bundle] = None, var dimension: Int = -11) extends ProductHash with Monoid[Sheaf] {//TODO turn into monad so we can flatmap collection
  //TODO call the method that invokes minhash/combine 'section' https://arxiv.org/pdf/0907.0995.pdf
  def empty = Sheaf()
  def combine(x: Sheaf, y: Sheaf = this) = Sheaf(None, x.dimension - 1)
}

