package org.constellation.consensus
import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props}

class RoundManager extends Actor with ActorLogging {
  import RoundManager._

  val rounds: Map[RoundId, ActorRef] = Map()

  override def receive: Receive = {
    case StartBlockCreationRound => {
      val id = generateRoundId
      rounds + (id -> generateRoundActor(id))
      // TODO: What about facilitators? dao.threadSafeTipService.pull()
    }

    case NotifyFacilitators(id) => {
      // TODO: Verify that parent is node or supervisor
      context.parent ! NotifyFacilitators(id)
    }

    case _ => log.info("Received unknown message")
  }
}

object RoundManager {
  def props: Props = Props(new RoundManager)

  def generateRoundId: RoundId =
    RoundId(java.util.UUID.randomUUID().toString)

  def generateRoundActor(roundId: RoundId)(implicit context: ActorContext) =
    context.actorOf(Round.props(roundId))
}
