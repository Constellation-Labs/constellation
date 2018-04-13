package org.constellation.state

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.wallet.KeyUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

class ChainStateManagerTest extends TestKit(ActorSystem("ChainStateManagerTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "handleAddBlock" should "work correctly" in {
    // TODO
  }

  "handleCreateBlockProposal" should "work correctly" in {
    // TODO
  }
}
