package org.constellation.blockchain

import java.security.PublicKey

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.constellation.Fixtures
import org.constellation.Fixtures.tx
import org.constellation.actor.Receiver
import org.constellation.blockchain.Consensus.PerformConsensus
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ProtocolInterface
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, GivenWhenThen}

import scala.collection.mutable
import scala.concurrent.duration._

class TestConsensusActor(override val peers: scala.collection.mutable.Set[ActorRef] = scala.collection.mutable.Set.empty[ActorRef]) extends Receiver with Consensus with PeerToPeer with ProtocolInterface {
  val publicKey: PublicKey = Fixtures.tempKey.getPublic
}

class ConsensusTest extends TestKit(ActorSystem("ConsensusTest")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  trait WithConsensusActor {
    val probe = TestProbe()
    val consensusActor = system.actorOf(Props(new TestConsensusActor(mutable.Set(probe.ref))))
  }

  "A Consensus actor" should "reply with the same tx when a tx is received" in new WithConsensusActor {
    consensusActor ! tx

    expectMsgPF() {
      case tx: Tx => assert(tx.hash == "")
    }
  }

/*
  "A Consensus actor" should "reply with the same sign tx when a tx is received" in new WithConsensusActor {
    consensusActor ! signTx

    expectMsgPF() {
      case tx: Tx => assert(tx.id == "")//TODO make more robust
    }
  }
*/

  "A Consensus actor" should "reply with the new block when a consensus request is finished" in new WithConsensusActor {
    consensusActor ! PerformConsensus
    probe.expectMsg(Block("hashPointer", 0L, "signature", mutable.HashMap[ActorRef, Option[BlockData]](), 0L))
  }
}
