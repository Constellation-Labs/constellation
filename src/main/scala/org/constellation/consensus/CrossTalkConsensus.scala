package org.constellation.consensus

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{BackoffOpts, BackoffSupervisor}
import org.constellation.consensus.CrossTalkConsensus.{NotifyFacilitators, ParticipateInBlockCreationRound, StartNewBlockCreationRound}
import org.constellation.consensus.Round._
import org.constellation.consensus.RoundManager.{BroadcastLightTransactionProposal, BroadcastSelectedUnionBlock, BroadcastUnionBlockProposal}
import org.constellation.{ConfigUtil, DAO}

import scala.concurrent.duration._

class CrossTalkConsensus(remoteSenderSupervisor: ActorRef)(implicit dao: DAO)
    extends Actor
    with ActorLogging {
  val roundTimeout: FiniteDuration = ConfigUtil.getDurationFromConfig(
    "constellation.consensus.form-checkpoint-blocks-timeout",
    60.second
  )

  val roundManagerProps: Props = RoundManager.props(roundTimeout)
  val roundManagerSupervisor = BackoffSupervisor.props(
    BackoffOpts.onFailure(
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

    case cmd: BroadcastLightTransactionProposal =>
      remoteSenderSupervisor ! cmd

    case cmd: LightTransactionsProposal ⇒
      roundManager ! cmd

    case cmd: UnionBlockProposal =>
      roundManager ! cmd

    case cmd: SelectedUnionBlock =>
      roundManager ! cmd

    case cmd: BroadcastUnionBlockProposal =>
      remoteSenderSupervisor ! cmd

    case cmd: NotifyFacilitators =>
      remoteSenderSupervisor ! cmd

    case cmd: BroadcastSelectedUnionBlock =>
      remoteSenderSupervisor ! cmd

    case cmd => log.warning(s"Received unknown message $cmd")
  }
}

object CrossTalkConsensus {
  def props(remoteSenderSupervisor: ActorRef)(implicit dao: DAO): Props =
    Props(new CrossTalkConsensus(remoteSenderSupervisor))

  sealed trait CrossTalkConsensusCommand

  case class NotifyFacilitators(roundData: RoundData) extends CrossTalkConsensusCommand

  case class ParticipateInBlockCreationRound(roundData: RoundData) extends CrossTalkConsensusCommand

  case object StartNewBlockCreationRound extends CrossTalkConsensusCommand
}
