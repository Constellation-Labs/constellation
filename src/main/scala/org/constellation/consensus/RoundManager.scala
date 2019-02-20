package org.constellation.consensus
import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props}

class RoundManager extends Actor with ActorLogging {
  import RoundManager._

  val rounds: Map[RoundId, ActorRef] = Map()

  override def receive: Receive = {
    case StartBlockCreationRound =>
      startBlockCreationRound

    case cmd: NotifyFacilitators =>
      passToParentActor(cmd)

    case cmd: BroadcastProposal =>
      passToParentActor(cmd)

    case cmd: ReceivedProposal =>
      passToRoundActor(cmd)

    case cmd: BroadcastMajorityUnionedBlock =>
      passToParentActor(cmd)

    case cmd: ReceivedMajorityUnionedBlock =>
      passToRoundActor(cmd)
  }

  def startBlockCreationRound: Map[RoundId, ActorRef] = {
    val id = generateRoundId
    rounds + (id -> generateRoundActor(id))
  }

  def passToRoundActor(cmd: RoundCommand): Unit = {
    rounds.get(cmd.roundId).fold()(_ ! cmd)
  }

  def passToParentActor(cmd: RoundCommand): Unit = {
    context.parent ! cmd
  }
}

object RoundManager {
  def props: Props = Props(new RoundManager)

  def generateRoundId: RoundId =
    RoundId(java.util.UUID.randomUUID().toString)

  def generateRoundActor(roundId: RoundId)(implicit context: ActorContext): ActorRef =
    context.actorOf(Round.props(roundId))
}
