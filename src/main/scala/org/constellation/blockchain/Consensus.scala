package org.constellation.blockchain

import akka.remote.ContainerFormats.ActorRef
import org.constellation.actor.Receiver
import org.constellation.blockchain.Consensus.PerformConsensus
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ProtocolInterface

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
  def performConsensus(actorRefs: Seq[ActorRef]): CheckpointBlock = CheckpointBlock("hashPointer", 0L, "signature")
}

/**
  * Interface for Consensus. This needs to be its own actor and have a type corresponding to the metamorphism that
  * implements proof of meme. The actual rep/dht will be dispatched via this actor.
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
      val newBlock = performConsensus(Nil)//TODO make this a future/non-blocking
        chain.globalChain.append(newBlock)
        broadcast(newBlock)

    case checkpointMessage: CheckpointMessage =>
      if (validBlockData(checkpointMessage) && isDelegate) {

      }
  }
}
