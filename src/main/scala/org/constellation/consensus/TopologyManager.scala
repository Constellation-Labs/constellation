package org.constellation.consensus

import akka.actor.FSM
import akka.actor.Status.Success
import cats.Functor
import cats.implicits._
import org.constellation.primitives.Schema.{Event, NodeState, Offline, SyncChain}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Wyatt on 5/18/18.
  */
object TopologyManager extends Recursive {
   val coAlgebra: Sheaf => Cell[Sheaf] = s => Cell(s)
    //TODO cata: flatten over future/promise, essentially just return the value of the raw fiber data after validation

  val algebra: Cell[Sheaf] => Sheaf = {
    case _ => Bundle()
  }
  //TODO ana: enque from mempool into this future, begin validation process with the metadata from last block B, pull data from mempool manager, via ask

  /**
    * This is the source of a stream io monad, also collapses into new source for new stream (hash pointer)
    */
  def hylo[F[_]: Functor, A, B](f: F[B] => B)(g: A => F[A]): A => B = a => f(g(a) map hylo(f)(g))
  def openStream(sheaf: Sheaf) = hylo(algebra)(coAlgebra).apply(sheaf)
}

class TopologyManager[S, D] extends FSM[NodeState, Event]{
  //TODO use akka streams/typed streams
  // extends PeerToPeer with MempoolManager

  startWith(Offline, SyncChain)

  when(Offline){
    case Event(bundle: Bundle, _) =>
//      TopologyManager.hylo()()
      //ringBuffer.parFold(hyloMorphism(bundle))
      stay()
    case Event(Offline, _) => goto(Offline)
  }
  initialize()
}
