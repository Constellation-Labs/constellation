package org.constellation.state

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import org.constellation.consensus.Consensus.ProposedBlockUpdated
import org.constellation.primitives.Chain.Chain
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.ChainStateManager.BlockAddedToChain
import org.constellation.state.MemPoolManager.RemoveConfirmedTransactions
import org.constellation.utils.TestNode
import org.constellation.wallet.KeyUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContextExecutor

class ChainStateManagerTest extends TestKit(ActorSystem("ChainStateManagerTest")) with FlatSpecLike with BeforeAndAfterAll {

  implicit val materializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "handleAddBlock" should "work correctly" in {
    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()

    val transaction1 =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val genesisBlock = Block("gen", 0, "", Set(), 0L, Seq())
    val newBlock = Block("sig", 0, "", Set(), 1L, Seq(transaction1))

    val chain = Chain(Seq(genesisBlock))
    val memPoolManager = TestProbe()
    val replyTo = TestProbe()
    val updatedChain = ChainStateManager.handleAddBlock(chain, newBlock, memPoolManager.ref, replyTo.ref)

    val expectedChain = Chain(Seq(genesisBlock, newBlock))

    assert(updatedChain == expectedChain)

    memPoolManager.expectMsg(RemoveConfirmedTransactions(newBlock.transactions))

    replyTo.expectMsg(BlockAddedToChain(newBlock))

  }

  "handleCreateBlockProposal" should "work correctly" in {
//    val node1 = TestProbe()
//    val node2 = TestProbe()
//    val node3 = TestProbe()
//    val node4 = TestProbe()
//    val node5 = TestProbe()

    val node1 = TestNode()
    val node2 = TestNode()
    val node3 = TestNode()
    val node4 = TestNode()
    val node5 = TestNode()

    val genesisBlock = Block("gen", 0, "", Set(), 0L, Seq())

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()
    val node3KeyPair = KeyUtils.makeKeyPair()
    val node4KeyPair = KeyUtils.makeKeyPair()

    val transaction1 =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val transaction2 =
      Transaction.senderSign(Transaction(1L, node2KeyPair.getPublic, node4KeyPair.getPublic, 14L), node2KeyPair.getPrivate)

    val transaction3 =
      Transaction.senderSign(Transaction(2L, node4KeyPair.getPublic, node1KeyPair.getPublic, 2L), node4KeyPair.getPrivate)

    val transaction4 =
      Transaction.senderSign(Transaction(3L, node3KeyPair.getPublic, node2KeyPair.getPublic, 20L), node3KeyPair.getPrivate)

    val chain = Chain(Seq(genesisBlock))

    val memPools = HashMap(
        node1.udpAddress -> Seq(transaction1, transaction3),
        node2.udpAddress -> Seq(transaction2),
        node3.udpAddress -> Seq(transaction3, transaction2),
        node4.udpAddress -> Seq(transaction4))

    val replyTo = TestProbe()

    ChainStateManager.handleCreateBlockProposal(memPools, chain, 1L, replyTo.ref)

    replyTo.expectMsg(ProposedBlockUpdated(Block(genesisBlock.signature, 1, "",
      genesisBlock.clusterParticipants, 1L, Seq(transaction1, transaction2, transaction3, transaction4))))

  }
}
