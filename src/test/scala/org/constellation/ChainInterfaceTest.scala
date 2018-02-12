package org.constellation

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.constellation.actor.Receiver
import org.constellation.blockchain.{CheckpointBlock, DAG}
import org.constellation.p2p.PeerToPeer
import org.constellation.p2p.PeerToPeer.{AddPeer, GetPeers, HandShake}
import org.constellation.rpc.ProtocolInterface
import org.constellation.rpc.ProtocolInterface.{FullChain, GetChain, GetLatestBlock, ResponseBlock}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, GivenWhenThen, Matchers}
import Fixtures._

class ProtocolInterfaceActor(val publicKey: String) extends
  Receiver with ProtocolInterface with PeerToPeer

class ChainInterfaceTest extends TestKit(ActorSystem("BlockChain")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll with Matchers {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  trait WithTestActor {
    val blockChain = DAG
    val blockChainCommunicationActor: ActorRef = system.actorOf(Props(classOf[ProtocolInterfaceActor], blockChain))
  }

  "A BlockChainCommunication actor" should "send the blockchain to anybody that requests it" in new WithTestActor {
    blockChainCommunicationActor ! GetChain
    expectMsg(FullChain(genesisBlock :: Nil))
  }

  it should "send the latest block, and nothing more for a QueryLatest request" in new WithTestActor {
    When("we query for a single block")
    blockChainCommunicationActor ! GetLatestBlock

    Then("we only expect the latest block")
    expectMsg(genesisBlock)
  }

  it should "attach a block to the current chain when it receives a new BlockChain which has exactly 1 new block" in new WithTestActor {

    Given("and a new chain containing an extra block")
    val nextBlock = checkpointBlock
    val oldBlocks = genesisBlock

    When("we receive a message with the longer chain")
    blockChainCommunicationActor ! nextBlock
    blockChainCommunicationActor ! GetLatestBlock

    Then("the new block should be added to the chain, and a broadcast should be sent")
    expectMsgPF() {
      case checkpointBlock: CheckpointBlock => checkpointBlock shouldEqual nextBlock
    }
  }

  it should "replace the chain if more than 1 new block is received" in new WithTestActor {
    Given("A blockchain with 3 new blocks")
    val longerChain = checkpointBlock :: genesisBlock :: Nil
    blockChainCommunicationActor ! AddPeer(testActor.path.toStringWithoutAddress)
    expectMsg(HandShake)
    expectMsg(GetPeers)

    When("we receive this longer chain")
    blockChainCommunicationActor ! FullChain(longerChain)

    Then("The chain should be replaced, and a broadcast should be sent")
    expectMsg(FullChain(longerChain))

  }


  it should "do nothing if the received chain is valid but shorter than the current one" in new WithTestActor  {
    Given("the old blockchain and a current blockchain which is longer")
    val longerChain = checkpointBlock :: genesisBlock :: Nil

    blockChainCommunicationActor ! FullChain(longerChain)

    When("we receive the old blockchain")
    blockChainCommunicationActor ! FullChain(genesisBlock :: Nil)

    Then("We expect the message to be discarded")
    blockChainCommunicationActor ! GetChain
    expectMsg(FullChain(longerChain))
  }

  it should "query for the full chain when we receive a single block that is further ahead in the chain" in new WithTestActor {
    Given("a later version of the blockchain which is 2 blocks ahead")
    val longerChain = checkpointBlock :: genesisBlock :: Nil

    blockChainCommunicationActor ! HandShake

    When("we receive the head of this blockchain")
    blockChainCommunicationActor ! ResponseBlock(longerChain.head)

    Then("expect a query for the full blockchain")
    expectMsg(GetChain)

    Then("we expect the blockchain to be unchanged")
    blockChainCommunicationActor ! GetChain
    expectMsg(FullChain(longerChain))

  }

}
