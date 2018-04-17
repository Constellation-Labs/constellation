package org.constellation.state

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.consensus.Consensus.MemPoolUpdated
import org.constellation.primitives.Transaction
import org.constellation.wallet.KeyUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.collection.mutable.ListBuffer

class MemPoolManagerTest extends TestKit(ActorSystem("MemPoolManagerTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "handleAddTransaction" should "work correctly" in {

    val memPool = new ListBuffer[Transaction]

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()

    val transaction1 =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val result = MemPoolManager.handleAddTransaction(memPool, transaction1)

    val expected = new ListBuffer[Transaction].+=(transaction1)

    assert(result == expected)
  }

  "handleGetMemPool" should "work correctly" in {
    var memPool = new ListBuffer[Transaction]

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()
    val node3KeyPair = KeyUtils.makeKeyPair()

    val transaction1 =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val transaction2 =
      Transaction.senderSign(Transaction(1L, node2KeyPair.getPublic, node1KeyPair.getPublic, 33L), node2KeyPair.getPrivate)

    val transaction3 =
      Transaction.senderSign(Transaction(2L, node3KeyPair.getPublic, node2KeyPair.getPublic, 10L), node3KeyPair.getPrivate)

    memPool.+=(transaction1)
    memPool.+=(transaction2)
    memPool.+=(transaction3)

    val replyTo = TestProbe()

    MemPoolManager.handleGetMemPool(memPool, replyTo.ref, 0L, 2)

    val expectedMemPoolSample = Seq(transaction1, transaction2)

    replyTo.expectMsg(MemPoolUpdated(expectedMemPoolSample, 0L))
  }

  "handleRemoveConfirmedTransactions" should "work correctly" in {
    var memPool = new ListBuffer[Transaction]

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()
    val node3KeyPair = KeyUtils.makeKeyPair()

    val transaction1 =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val transaction2 =
      Transaction.senderSign(Transaction(1L, node2KeyPair.getPublic, node1KeyPair.getPublic, 33L), node2KeyPair.getPrivate)

    val transaction3 =
      Transaction.senderSign(Transaction(2L, node3KeyPair.getPublic, node2KeyPair.getPublic, 10L), node3KeyPair.getPrivate)

    memPool.+=(transaction1)
    memPool.+=(transaction2)
    memPool.+=(transaction3)

    MemPoolManager.handleRemoveConfirmedTransactions(Seq(transaction1, transaction3), memPool)

    val expectedMemPool = new ListBuffer[Transaction]
    expectedMemPool.+=(transaction2)

    assert(memPool == expectedMemPool)
  }
}
