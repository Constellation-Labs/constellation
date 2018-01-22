package org.constellation.blockchain

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.constellation.Fixtures.{signTx, tx}
import org.constellation.actor.Receiver
import org.constellation.blockchain.Consensus.MineBlock
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ProtocolInterface
import org.constellation.rpc.ProtocolInterface.ResponseBlock
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, GivenWhenThen}

class TestConsensusActor extends Receiver with Consensus with PeerToPeer with ProtocolInterface {
  var blockChain = Chain("id")
}

class ConsensusTest extends TestKit(ActorSystem("BlockChain")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  trait WithConsensusActor {
    val consensusActor = system.actorOf(Props[TestConsensusActor])
  }

  "A Consensus actor" should "reply with the same tx when a tx is received" in new WithConsensusActor {
    consensusActor ! tx

    expectMsgPF() {
      case tx: Tx => assert(tx.id == "")
    }
  }

  "A Consensus actor" should "reply with the same sign tx when a tx is received" in new WithConsensusActor {
    consensusActor ! signTx


    expectMsgPF() {
      case tx: Tx => assert(tx.id == "")//TODO make more robust
    }
  }

  "A Consensus actor" should "reply with the new block when a consensus request is finished" in new WithConsensusActor {
    consensusActor ! MineBlock


    expectMsgPF() {
      case ResponseBlock(block) => assert(block.hash == "1b10da175ac6e6702aa1d6041b77f0b4dfb267dbd3aa02b01547a34035d9f1de")//TODO hash here non deterministic, use ryle's
    }
  }
}
