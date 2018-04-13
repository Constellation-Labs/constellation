package org.constellation.state

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.wallet.KeyUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

class MemPoolManagerTest extends TestKit(ActorSystem("MemPoolManagerTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "handleAddTransaction" should "work correctly" in {
    // TODO
  }

  "handleGetMemPool" should "work correctly" in {
    // TODO
  }

  "handleRemoveConfirmedTransactions" should "work correctly" in {
    // TODO
  }
}
