package org.constellation.primitives

import akka.actor.ActorRef
import org.constellation.primitives.Schema.ResolvedTX

object TransactionValidation {


  def validateTransaction(dbActor: ActorRef, tx: ResolvedTX): Boolean = {

    true
  }


}
