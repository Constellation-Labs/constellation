package org.constellation.consensus
import akka.actor.{Actor, FSM}
import org.constellation.primitives.Block
import org.constellation.state.RateLimitedFSM

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Wyatt on 5/10/18.
  */
sealed trait State
case object Online extends State
case object Offline extends State

sealed trait Event
case object Tx extends Event
case object Bundle extends Event
case object SyncChain extends Event
case object Sleep extends Event

object NodeStateManager {
  def performConsensus(): Future[Unit] =
    /*
    should be Future[Block] the result of consensus
     */
    Future()

  def broadcast(tx: Event*): Unit = {
    /*
    Use local rep scored to selectively sample neighbors according to a distribution
     */
  }

  def syncChain(): Future[Unit] =
    /*
    chain sync logic in consensus actor should go here
     */
    Future()

  def validBundle(cp: Event*): Boolean = true

  def validTx(tx: Event): Boolean = true

  def setUp(): Unit = {
    /*
    instantiate actors for sub processes
     */
  }
}

class NodeStateManager extends RateLimitedFSM[State, Event] {
  import NodeStateManager._
  //TODO mixin AtMostOnce FSM StateFunction
  startWith(Offline, SyncChain)

  when(Online)(rateLimit {
    case Event(cpBlock@Bundle, _) =>
      if (validBundle(cpBlock)) broadcast(cpBlock)
      else performConsensus()
      stay()

    case Event(tx@Tx, _) =>
      if (validTx(tx)) broadcast(tx)
      //else TODO put instaban gossip here
      stay()

    case Event(Offline, _) => goto(Offline)
  })

  when(Offline)(rateLimit {
    case Event(Online, _) => goto(Online)

    case Event(tx@Tx, _) =>
      broadcast(tx)
      stay
  })

  onTransition {
    case Offline -> Online =>

      syncChain()

    case Online -> Offline =>
      broadcast(Sleep)
  }

  whenUnhandled {
    case Event(e, s) ⇒
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  initialize()
}