package org.constellation.consensus
import akka.actor.{Actor, ActorLogging, Props}

class Round(id: RoundId) extends Actor with ActorLogging {

  override def preStart(): Unit = {
    context.parent ! NotifyFacilitators(id)

    // TODO: pull transactions and broadcast proposal
    // dao.threadSafeTXMemPool.pull(dao.minCheckpointFormationThreshold)
  }

  override def receive: Receive = {
    case _ => log.info("Received unknown message")
  }

}

object Round {
  def props(id: RoundId): Props = Props(new Round(id))
}
