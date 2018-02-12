package org.constellation.blockchain

import akka.remote.ContainerFormats.ActorRef
import org.constellation.actor.Receiver
import org.constellation.blockchain.Consensus.PerformConsensus
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ProtocolInterface

import scala.concurrent.Future

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
  * Interface for Consensus. We might want this to be its own actor if it affects transaction bandwidth
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
      val newBlock = Future.apply(performConsensus(Nil))
      newBlock.foreach { block =>
        chain.globalChain.append(block)
        broadcast(block)
        sender() ! block
      }

    case checkpointMessage: CheckpointMessage =>
      if (validBlockData(checkpointMessage) && isDelegate) {

      }
  }
}
