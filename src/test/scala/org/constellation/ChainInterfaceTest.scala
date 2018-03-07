package org.constellation

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.constellation.actor.Receiver
import org.constellation.blockchain.{CheckpointBlock, DAG}
import org.constellation.p2p.PeerToPeer
import org.constellation.p2p.PeerToPeer.{AddPeer, GetPeers, HandShake, PeerRef}
import org.constellation.rpc.ProtocolInterface
import org.constellation.rpc.ProtocolInterface.{FullChain, GetChain, GetLatestBlock, ResponseBlock}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, GivenWhenThen, Matchers}
import Fixtures.{genesisBlock, _}

import scala.collection.mutable.ListBuffer

class ProtocolInterfaceActor(val publicKey: String = "") extends
  Receiver with ProtocolInterface with PeerToPeer{
}

class ChainInterfaceTest extends TestKit(ActorSystem("ChainInterfaceTest")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll with Matchers {
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  trait WithTestActor {
    val blockChainCommunicationActor = TestActorRef(Props(new ProtocolInterfaceActor("")))

    val longerChain: List[CheckpointBlock] =  checkpointBlock :: genesisBlock :: Nil
    val shorterChain: List[CheckpointBlock] = genesisBlock :: Nil
  }

  "A BlockChainCommunication actor" should "send the blockchain to anybody that requests it" in new WithTestActor {
    When("an empty chain is queried")
    blockChainCommunicationActor ! GetChain
    Then("we should receive this empty DAG's globalChain")
    expectMsg(FullChain(ListBuffer.empty[CheckpointBlock]))
  }

  it should "send the latest block, and nothing more for a QueryLatest request" in new WithTestActor {
    When("we query for a single block")
    blockChainCommunicationActor ! GetLatestBlock

    Then("we only expect the latest block")
    expectMsg(None)
  }

  it should "attach a block to the current chain when it receives a new block" in new WithTestActor {

    When("we receive a message with the longer chain")
    blockChainCommunicationActor ! genesisBlock
    blockChainCommunicationActor ! GetLatestBlock

    Then("the new block should be added to the chain, and a broadcast should be sent")
    expectMsg(genesisBlock)
  }



  it should "do nothing if the received chain is valid but shorter than the current one" in new WithTestActor  {
    Given("the old blockchain and a current blockchain which is longer")
    When("we receive the old blockchain")
    blockChainCommunicationActor ! FullChain(shorterChain)
    expectMsg(None)
  }
}
