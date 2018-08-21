package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema.{ResolvedTX, SendToAddress, TX, TransactionData}

class NodeManager(keyManager: ActorRef, metricsManager: ActorRef) extends Actor {

  override def receive: Receive = {

    case s: SendToAddress =>

      metricsManager ! IncrementMetric("sentTransactions")
      keyManager ! s

  }
}

