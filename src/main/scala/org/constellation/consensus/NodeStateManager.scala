package org.constellation.consensus
import java.security.KeyPair

import akka.actor.{ActorSystem, FSM, Props}
import org.constellation.consensus.TopologyManager
import org.constellation.primitives.Schema.{Event, _}
import org.constellation.wallet.KeyUtils.makeKeyPair
/**
  * Created by Wyatt on 5/10/18.
  */
object NodeStateManager {
//  def propagate[B <: Bundle](bundle: B) = {}

  /**
    * TODO put instantiation in ConstellationNode App into NodeStateManager object
    * @param args
    */
  def apply(args: Array[String]) = {

  }
}

class NodeStateManager(val keyPair: KeyPair = makeKeyPair(), system: ActorSystem) extends FSM[NodeState, Event] {
  import NodeStateManager._
  //TODO make both akkaStreams FSM and self reference
  val p2p = system.actorOf(Props(new TopologyManager))
  val validator = system.actorOf(Props(new ManifoldManager))

  startWith(Offline, SyncChain)

  when(Offline){
    case Event(Online, _) => goto(Online)
//    case Event(bundle: Bundle, _) =>
//      propagate(bundle)
//      stay()
  }

  onTransition {
    case Offline -> Online => toggleNetworkConnection(Online)
    case Online -> Offline => toggleNetworkConnection(Online)
  }

  whenUnhandled {
    case Event(e, s) ⇒
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  def toggleNetworkConnection(nodeState: NodeState): Unit = {
    p2p ! nodeState
    validator ! nodeState
  }
  initialize()
}