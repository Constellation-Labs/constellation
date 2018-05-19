package org.constellation.consensus
import java.security.{KeyPair, PublicKey}

import akka.actor.{Actor, ActorRef, ActorSystem, FSM, PoisonPill, Props}
import org.constellation.p2p.PeerToPeer
import org.constellation.primitives.Block
import org.constellation.state.RateLimitedFSM
import org.constellation.transaction.AtomicTransaction.TransactionInputData
import org.constellation.wallet.KeyUtils.makeKeyPair

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.constellation.{p2p, _}
import org.constellation.primitives.Schema._
import org.constellation.util.POWSignHelp
import org.constellation.primitives.Schema.Event
/**
  * Created by Wyatt on 5/10/18.
  */



/*
TODO put instantiation in ConstellationNode App into NodeStateManager object
 */
object NodeStateManager {
  def propagate[B <: Bundle](bundle: B) = {}

  /**
    * Main control flow
    * @param args
    */
  def apply(args: Array[String]) = {

  }
}

class NodeStateManager(val keyPair: KeyPair = makeKeyPair(), system: ActorSystem) extends FSM[NodeState, Event] {
  import NodeStateManager._
  //TODO make both akkaStreams FSM and self reference
  val p2p = system.actorOf(Props(new Topology))
  val validator = system.actorOf(Props(new Manifold))

  startWith(Offline, SyncChain)

  when(Offline){
    case Event(Online, _) => goto(Online)
    case Event(bundle: Bundle, _) =>
      propagate(bundle)
      stay()
  }

  onTransition {
    case Offline -> Online => linkNetwork(Online)
    case Online -> Offline => linkNetwork(Online)
  }

  whenUnhandled {
    case Event(e, s) ⇒
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  def linkNetwork(nodeState: NodeState): Unit = {
    p2p ! nodeState
    validator ! nodeState
  }
  initialize()
}