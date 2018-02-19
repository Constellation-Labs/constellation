package org.constellation.blockchain

import akka.actor.ActorRef
//import akka.remote.ContainerFormats.ActorRef
import org.constellation.actor.Receiver
import org.constellation.blockchain.Consensus.PerformConsensus
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ProtocolInterface

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
  * Ideally I'd like this to be instantiated as a Metamorphism.
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

/**
  * Interface for Consensus. This needs to be its own actor and have a type corresponding to the metamorphism that
  * implements proof of meme. The actual rep/dht will be dispatched via this actor.
  *
  * We need to implement this to follow the signature below
  *
  *
  * def metaSimple[F[_] : Functor, A, B](f: F[B] => B)(g: A => F[A]): A => B =
      cata(g) andThen ana(f)
  *
  * and also figure out a way to define the algebras in terms of a bialgebra given that http://comonad.com/reader/2009/recursion-schemes/
  * and we also need a vector space for proof of meme https://en.wikipedia.org/wiki/Bialgebra
  */
trait Consensus {
  this: ProtocolInterface with PeerToPeer with Receiver =>
  import Consensus.performConsensus
  import ProtocolInterface.{isDelegate, validBlockData}

  receiver {
    /**
      * Right now we will send a message to kick off a consensus round. This will eventually be tripped by a counter in
      * ProtocolInterface.
      *
      */
    case PerformConsensus =>
      val newBlock = performConsensus(Nil) //TODO make this a future/non-blocking
      chain.globalChain.append(newBlock)
      broadcast(newBlock)

    case checkpointMessage: CheckpointMessage =>
      if (validBlockData(checkpointMessage) && isDelegate) {

      }
  }
}
