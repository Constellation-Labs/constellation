package org.constellation.consensus
import akka.actor.ActorSystem
import akka.testkit.{TestFSMRef, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
/**
  * Created by Wyatt on 5/10/18.
  */
class NodeStateManagerTest extends TestKit(ActorSystem("ConsensusTest")) with FlatSpecLike with BeforeAndAfterAll {

  "Gossiper gossiper FSM" should "initialize correctly" in {
    val gossiper = TestFSMRef(new NodeStateManager)
    assert(gossiper.stateName == Offline)
  }

  "Gossiper gossiper FSM" should "transition on correctly" in {
    val gossiper = TestFSMRef(new NodeStateManager)
    gossiper ! Online
    assert(gossiper.stateName == Online)

  }

//  "Gossiper gossiper FSM" should "transition on then off correctly" in {
//    val gossiper = TestFSMRef(new Gossip)
//    assert(gossiper.stateName == Gossiper)
//  }

}
