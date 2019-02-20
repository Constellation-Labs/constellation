package org.constellation.consensus
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{Backoff, BackoffSupervisor}
import org.constellation.DAO

import scala.concurrent.duration._

class Node extends Actor with ActorLogging {
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

  val nodeRemoteSender: Props = HTTPNodeRemoteSender.props
  val nodeRemoteSupervisor: Props = BackoffSupervisor.props(
    Backoff.onFailure(
      roundManagerProps,
      childName = "node-remote",
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    )
  )
  val nodeRemote: ActorRef =
    context.actorOf(nodeRemoteSupervisor, name = "node-remote-supervisor")

  override def receive: Receive = {
    case StartBlockCreationRound =>
      roundManager ! StartBlockCreationRound

    case cmd: ReceivedProposal =>
      roundManager ! cmd

    case cmd: ReceivedMajorityUnionedBlock =>
      roundManager ! cmd

    case cmd: NotifyFacilitators =>
      nodeRemote ! cmd

    case cmd: BroadcastProposal =>
      nodeRemote ! cmd

    case cmd: BroadcastMajorityUnionedBlock =>
      nodeRemote ! cmd

    case _                       => log.info("Received unknown message")
  }
}

object Node {
  def props(implicit dao: DAO): Props = Props(new Node)
}
