package org.constellation.blockchain

import akka.actor.{ActorRef, FSM}
import akka.actor.FSM.State
import scala.collection.mutable

/**
  * Interface for Consensus. This needs to be its own actor and have a type corresponding to the metamorphism that
  * implements proof of meme. The actual rep/dht will be dispatched via this actor.
  *
  * We need to implement this to follow the signature below
  *
  *
  * def metaSimple[F[_] : Functor, A, B](f: F[B] => B)(g: A => F[A]): A => B =
  *   cata(g) andThen ana(f)
  *
  * and also figure out a way to define the algebras in terms of a bialgebra given that http://comonad.com/reader/2009/recursion-schemes/
  * and we also need a vector space for proof of meme https://en.wikipedia.org/wiki/Bialgebra
  *
  * for ref http://www.cs.ox.ac.uk/jeremy.gibbons/publications/metamorphisms-scp.pdf
  */
object Consensus {
  case class PerformConsensus()

  /**
    * Just a stub for now, res will be checkpoint block which will be used by updateCache
    * @param actorRefs
    * @return
    */
  def performConsensus(actorRefs: Seq[ActorRef]): CheckpointBlock = CheckpointBlock("hashPointer", 0L, "signature", mutable.HashMap[ActorRef, Option[BlockData]](), 0L)
}

class Consensus[T,U](chain: DAG) extends FSM[State[T,U], Unit]