package org.constellation.blockchain

import java.security.PublicKey

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.constellation.Fixtures
import org.constellation.Fixtures.tx
import org.constellation.consensus.Consensus
import org.constellation.p2p.PeerToPeer
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, GivenWhenThen}

import scala.collection.mutable

/*
class ConsensusTest extends TestKit(ActorSystem("ConsensusTest")) with FlatSpecLike
with ImplicitSender with GivenWhenThen with BeforeAndAfterAll {


  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  trait WithConsensusActor {
    val probe = TestProbe()
  //  val consensusActor = system.actorOf(Props(new Consensus(mutable.Set(probe.ref))))
  }

  "A Consensus actor" should "reply with the same tx when a tx is received" in new WithConsensusActor {
  //  consensusActor ! tx

  }

  "A Consensus actor" should "reply with the same sign tx when a tx is received" in new WithConsensusActor {
    consensusActor ! signTx

    expectMsgPF() {
      case tx: Tx => assert(tx.id == "")//TODO make more robust
    }
  }

  "A Consensus actor" should "reply with the new block when a consensus request is finished" in new WithConsensusActor {
   // consensusActor ! PerformConsensus
    probe.expectMsg(Block("hashPointer", 0L, "signature", mutable.HashMap[ActorRef, Option[BlockData]](), 0L))
  }

}

  */
