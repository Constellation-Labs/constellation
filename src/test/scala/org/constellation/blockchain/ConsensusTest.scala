package org.constellation.blockchain

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.constellation.actor.Receiver
import org.constellation.rpc.ChainInterface.ResponseBlock
import org.constellation.blockchain.Consensus.MineBlock
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ChainInterface
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, GivenWhenThen}

class TestConsensusActor extends Receiver with Consensus with PeerToPeer with ChainInterface {
  var blockChain = Chain()
}

class ConsensusTest extends TestKit(ActorSystem("BlockChain")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  trait WithConsensusActor {
    val consensusActor = system.actorOf(Props[TestConsensusActor])
  }

  "A Mining actor" should "reply with the new block when a mining request is finished" in new WithConsensusActor {

    consensusActor ! MineBlock("testBlock")

    expectMsgPF() {
      case ResponseBlock(block) => assert(block.data == "testBlock")
    }

  }
}
