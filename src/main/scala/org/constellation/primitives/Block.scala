package org.constellation.primitives

import akka.actor.ActorRef
import org.constellation.primitives.Transaction.Transaction

object Block {

  case class Block(parentHash: String,
                   height: Long,
                   signature: String = "",
                   clusterParticipants: Seq[ActorRef] = Seq(),
                   round: Long = 0L,
                   transactions: Seq[Transaction] = Seq())

//  def hash(block: Block): String = (Seq(block.height, block.parentHash) ++ block.transactions.map((t) => Transaction.hash(t)))

}



