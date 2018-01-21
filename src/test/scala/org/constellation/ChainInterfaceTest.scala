package org.constellation

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.constellation.actor.Receiver
import org.constellation.rpc.ProtocolInterface.{QueryAll, QueryLatest, ResponseBlock, ResponseBlockChain}
import org.constellation.blockchain.Chain
import org.constellation.p2p.PeerToPeer
import org.constellation.p2p.PeerToPeer.{AddPeer, GetPeers, HandShake}
import org.constellation.rpc.ProtocolInterface
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, GivenWhenThen, Matchers}

class ProtocolInterfaceActor(var blockChain: Chain) extends
  Receiver with ProtocolInterface with PeerToPeer

class ChainInterfaceTest extends TestKit(ActorSystem("BlockChain")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll with Matchers {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  trait WithTestActor {
    val blockChain = Chain("id").addBlock("My test data")
    val blockChainCommunicationActor: ActorRef = system.actorOf(Props(classOf[ProtocolInterfaceActor], blockChain))
  }

  "A BlockChainCommunication actor" should "send the blockchain to anybody that requests it" in new WithTestActor {
    blockChainCommunicationActor ! QueryAll
    expectMsg(ResponseBlockChain(blockChain))
  }

  it should "send the latest block, and nothing more for a QueryLatest request" in new WithTestActor {
    When("we query for a single block")
    blockChainCommunicationActor ! QueryLatest

    Then("we only expect the latest block")
    expectMsg(ResponseBlock(blockChain.latestBlock))
  }

  it should "attach a block to the current chain when it receives a new BlockChain which has exactly 1 new block" in new WithTestActor {

    Given("and a new chain containing an extra block")
    val nextBlock = blockChain.generateNextBlock("Some more data")
    val oldBlocks = blockChain.blocks

    When("we receive a message with the longer chain")
    blockChainCommunicationActor ! ResponseBlock(nextBlock)
    blockChainCommunicationActor ! QueryAll

    Then("the new block should be added to the chain, and a broadcast should be sent")
    expectMsgPF() {
      case ResponseBlockChain(blockChain) => blockChain.blocks shouldEqual( nextBlock +: oldBlocks )
    }
  }

  it should "replace the chain if more than 1 new block is received" in new WithTestActor {
    Given("A blockchain with 3 new blocks")
    val blockData = Seq("aap", "noot", "mies")
    val longerChain = blockData.foldLeft(blockChain) { case (chain, data) =>
      chain.addBlock(data)
    }

    blockChainCommunicationActor ! AddPeer(testActor.path.toStringWithoutAddress)
    expectMsg(HandShake)
    expectMsg(GetPeers)

    When("we receive this longer chain")
    blockChainCommunicationActor ! ResponseBlockChain(longerChain)

    Then("The chain should be replaced, and a broadcast should be sent")
    expectMsg(ResponseBlockChain(longerChain))

  }


  it should "do nothing if the received chain is valid but shorter than the current one" in new WithTestActor  {
    Given("the old blockchain and a current blockchain which is longer")
    val oldBlockChain = blockChain
    val newBlockChain = oldBlockChain .addBlock("Some new data") .addBlock("And more")

    blockChainCommunicationActor ! ResponseBlockChain(newBlockChain)

    When("we receive the old blockchain")
    blockChainCommunicationActor ! ResponseBlockChain(oldBlockChain)

    Then("We expect the message to be discarded")
    blockChainCommunicationActor ! QueryAll
    expectMsg(ResponseBlockChain(newBlockChain))
  }

  it should "query for the full chain when we receive a single block that is further ahead in the chain" in new WithTestActor {
    Given("a later version of the blockchain which is 2 blocks ahead")
    val oldBlockChain = blockChain
    val newBlockChain = blockChain
      .addBlock("Some new data") .addBlock("And more")

    blockChainCommunicationActor ! HandShake

    When("we receive the head of this blockchain")
    blockChainCommunicationActor ! ResponseBlock(newBlockChain.latestBlock)

    Then("expect a query for the full blockchain")
    expectMsg(QueryAll)

    Then("we expect the blockchain to be unchanged")
    blockChainCommunicationActor ! QueryAll
    expectMsg(ResponseBlockChain(oldBlockChain))

  }

}
