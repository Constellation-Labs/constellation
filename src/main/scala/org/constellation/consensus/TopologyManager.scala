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
object TopologyManager {
  /**
    * This is the source of a stream io monad, also collapses into new source for new stream (hash pointer)
    */
//  def openStream(sheaf: Sheaf) = hylo(algebra)(coAlgebra).apply(sheaf)
}

class TopologyManager[S, D] extends FSM[NodeState, Event]{
  //TODO use akka streams/typed streams
  // extends PeerToPeer with MempoolManager

  startWith(Offline, SyncChain)

  when(Offline){
//    case Event(bundle: Bundle, _) =>
//      TopologyManager.hylo()()
      //ringBuffer.parFold(hyloMorphism(bundle))
//      stay()
    case Event(Offline, _) => goto(Offline)
  }
  initialize()
}
