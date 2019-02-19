package org.constellation.consensus
import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.{Backoff, BackoffSupervisor}
import org.constellation.DAO

import scala.concurrent.duration._

class Node(implicit dao: DAO) extends Actor with ActorLogging {
  private val roundManagerProps = RoundManager.props
  private val roundManagerSupervisor = BackoffSupervisor.props(
    Backoff.onFailure(
      roundManagerProps,
      childName = "round-manager",
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    )
  )
  private val roundSupervisorActor =
    context.actorOf(roundManagerSupervisor, name = "round-manager-supervisor")

  override def receive: Receive = {
    case StartBlockCreationRound => {
      roundSupervisorActor ! StartBlockCreationRound
    }

    case NotifyFacilitators(id) => {
      // TODO
      log.info(s"Notify facilitators of roundId=$id")
    }

    case _ => log.info("Received unknown message")
  }

}

object Node {
  def props(implicit dao: DAO): Props = Props(new Node)
}
