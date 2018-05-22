package org.constellation.consensus

import akka.actor.FSM
import cats.Functor
import cats.implicits._
import org.constellation.primitives.Schema._
/**
  * Created by Wyatt on 5/18/18.
  */
object Topology extends Recursive {
  def broadcast[T <: Event](tx: T*): Unit = {
    // TODO Use local rep scored to selectively sample neighbors according to a distribution
  }

  override def algebra[B](f: Functor[B]): B = {
    //TODO cata: flatten over future/promise, essentially just return the value of the raw fiber data after validation
  }

  override def coAlgebra[B](g: B): Functor[B] = {
    //TODO ana: enque from mempool into this future, begin validation process with the metadata from last block B, pull data from mempool manager, via ask
  }

  /**
    * This is the source of a stream io monad, also colapses into new source for new stream (hash pointer)
    */

  def hylo[F[_] : Functor, A, B](f: F[B] => B)(g: A => F[A]): A => B = a => f(g(a) map hylo(f)(g))
  //  def hylo[F[_]: Functor, A, B](a: A)(f: Algebra[F, B], g: Coalgebra[F, A])): B

}

class Topology[S, D] extends FSM[NodeState, Event]{//TODO use akka streams/typed streams
  // extends PeerToPeer with MempoolManager

  startWith(Offline, SyncChain)

  when(Offline){
    case Event(bundle: Bundle, _) =>
      //ringBuffer.parFold(hyloMorphism(bundle))
      stay()
    case Event(Offline, _) => goto(Offline)
  }
  initialize()
}
