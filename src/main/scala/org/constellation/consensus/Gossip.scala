package org.constellation.consensus
import akka.actor.{Actor, FSM}
import org.constellation.state.AtMostOnceFSM

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Wyatt on 5/10/18.
  */
sealed trait State
case object Gossiper extends State
case object Offline extends State

sealed trait Data
case object Tx extends Data
case object CheckpointBlock extends Data
case object BlockProposal extends Data
case object SyncChain extends Data
case object Sleep extends Data

object Gossip {
  def performConsensus(): Unit = {}

  def broadcast(tx: Data*): Unit = {}

  def syncChain(): Future[Unit] = Future()

  def validCheckpoint(cp: Data*): Boolean = true

  def validTx(tx: Data): Boolean = true

  def censorTx(): Unit = {}
}

class Gossip extends AtMostOnceFSM[State, Data] {
  import Gossip._
  //TODO mixin AtMostOnce FSM StateFunction
  startWith(Offline, Sleep)

  when(Gossiper) {
    case Event(cpBlock@CheckpointBlock, _) =>
      if (validCheckpoint(cpBlock)) broadcast(cpBlock)
      else performConsensus()
      stay()

    case Event(tx@Tx, _) =>
      if (validTx(tx)) broadcast(tx)
      //TODO put instaban gossip here
      stay()

    case Event(Offline, _) => goto(Offline)
  }

  when(Offline) {
    case Event(Gossiper, _) => goto(Gossiper)

    case Event(tx@Tx, _) =>
      broadcast(tx)
      stay
  }

  onTransition {
    case Offline -> Gossiper =>
      syncChain()

    case Gossiper -> Offline =>
      broadcast(Sleep)
  }

  whenUnhandled {
    case Event(e, s) ⇒
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  initialize()
}