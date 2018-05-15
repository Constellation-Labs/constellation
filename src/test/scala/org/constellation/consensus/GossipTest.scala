package org.constellation.consensus
import akka.actor.ActorSystem
import akka.testkit.{TestFSMRef, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
/**
  * Created by Wyatt on 5/10/18.
  */
class GossipTest extends TestKit(ActorSystem("ConsensusTest")) with FlatSpecLike with BeforeAndAfterAll {

  "Gossiper gossiper FSM" should "initialize correctly" in {
    val gossiper = TestFSMRef(new Gossip)
    assert(gossiper.stateName == Offline)
  }

  "Gossiper gossiper FSM" should "transition on correctly" in {
    val gossiper = TestFSMRef(new Gossip)
    gossiper ! Gossiper
    assert(gossiper.stateName == Gossiper)

  }

//  "Gossiper gossiper FSM" should "transition on then off correctly" in {
//    val gossiper = TestFSMRef(new Gossip)
//    assert(gossiper.stateName == Gossiper)
//  }

}
