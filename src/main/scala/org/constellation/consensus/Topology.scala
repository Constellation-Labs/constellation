package org.constellation.consensus

import java.security.KeyPair

import akka.actor.{Actor, FSM}
import org.constellation.primitives.Schema._
import org.constellation.state.{RateLimitedFSM, Semigroup}

/**
  * Created by Wyatt on 5/18/18.
  */
object Topology {
  def broadcast[T <: Event](tx: T*): Unit = {
    /*
    Use local rep scored to selectively sample neighbors according to a distribution
     */
  }

  /**
    * This is the source stream io monad, p2p => validator
    */
  def hyloMorphism[S <: Semigroup[Event]]() = {}
  //TODO PartialFunction? We want to handle ring buffering here, the ring buffer is a continuous reduce over
  // semigroup/monoid (when Bundle is still empty) operation, ring buffer of Bundle[Event] <: Future,
  // when the result of isvalid comes back from manifold, "pop" the z element and the Bundle together.
}

class Topology[S, D] extends FSM[S, D]{//TODO use akka streams/typed streams
  // extends PeerToPeer with MempoolManager
  //TODO hyloMorphism partial function

  //TODO put these in util
  //  def signBundle(keyPair: KeyPair, transaction: TXData) = TX(transaction.signed()(keyPair))
  //  def validTx(tx: TXData): Boolean = true
}
