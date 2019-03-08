package org.constellation.consensus
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{Backoff, BackoffSupervisor}
import akka.util.Timeout
import org.constellation.consensus.CrossTalkConsensus.{NotifyFacilitators, ParticipateInBlockCreationRound, StartNewBlockCreationRound}
import org.constellation.consensus.Round._
import org.constellation.consensus.RoundManager.{BroadcastTransactionProposal, BroadcastUnionBlockProposal}
import org.constellation.{ConfigUtil, DAO}

import scala.concurrent.duration._

class CrossTalkConsensus(remoteSenderSupervisor: ActorRef)(implicit dao: DAO) extends Actor with ActorLogging {


  val roundManagerProps: Props = RoundManager.props
  val roundManagerSupervisor: Props = BackoffSupervisor.props(
    Backoff.onFailure(
      roundManagerProps,
      childName = "round-manager",
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    )
  )
  val roundManager: ActorRef =
    context.actorOf(roundManagerSupervisor, name = "round-manager-supervisor")


  override def receive: Receive = {
    case StartNewBlockCreationRound =>
      roundManager ! StartNewBlockCreationRound
    case cmd: ParticipateInBlockCreationRound =>
      roundManager ! cmd
    case cmd: TransactionsProposal =>
      roundManager ! cmd
    case cmd: UnionBlockProposal =>
      roundManager ! cmd
    case cmd: BroadcastTransactionProposal =>
      remoteSenderSupervisor ! cmd
    case cmd: BroadcastUnionBlockProposal =>
      remoteSenderSupervisor ! cmd
    case cmd: NotifyFacilitators =>
      remoteSenderSupervisor ! cmd
    case cmd => log.warning(s"Received unknown message $cmd")
  }

}

object CrossTalkConsensus {
  sealed trait NodeCommand

  case class NotifyFacilitators(roundData: RoundData)
  case class ParticipateInBlockCreationRound(roundData: RoundData) extends NodeCommand
  case object StartNewBlockCreationRound extends NodeCommand

  def props(remoteSenderSupervisor: ActorRef)(implicit dao: DAO): Props =
    Props(new CrossTalkConsensus(remoteSenderSupervisor))
}
