package org.constellation.consensus

import java.security.KeyPair

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import org.constellation.consensus.Consensus._
import org.constellation.primitives.Block
import org.constellation.wallet.KeyUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.collection.mutable

class ConsensusTest extends TestKit(ActorSystem("ConsensusTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  trait WithConsensusActor {
      val memPoolManagerActor = TestProbe()
      val chainStateManagerActor = TestProbe()
      val keyPair: KeyPair = KeyUtils.makeKeyPair()
      val consensusActor: ActorRef =
        system.actorOf(Props(new Consensus(memPoolManagerActor.ref, chainStateManagerActor.ref, keyPair)))
  }

  "getFacilitators" should "give back the correct list of facilitators" in {

    val block = Block("hashPointer", 0L, "sig", Seq(), 0L, Seq())

    val facilitators = Consensus.getFacilitators(block)

    assert(facilitators == Seq())
  }

  "notifyFacilitatorsOfBlockProposal" should "notify all of the correct facilitators with the correct block" in {

    val block = Block("hashPointer", 0L, "sig", Seq(), 0L, Seq())

    val testProbe = TestProbe()

    val notify = Consensus.notifyFacilitatorsOfBlockProposal(block, Seq(), testProbe.ref)

    // TODO add assertions
  }

  "isFacilitator" should "return correctly if the actor is a facilitator" in {

    val testProbe = TestProbe()

    val isFacilitator = Consensus.isFacilitator(Seq(), testProbe.ref)

    // TODO add assertions
  }

  "getConsensusBlock" should "return the correct consensus block or not based on the state of consensus proposals" in {

    val testProbe = TestProbe()

    val consensusBlock: Option[Block] = Consensus.getConsensusBlock(mutable.HashMap(), Seq())

    // TODO add assertions
  }

  "A StartConsensusRound Message" should "be handled correctly" in new WithConsensusActor {

    consensusActor ! StartConsensusRound(Block("hashPointer", 0L, "sig", Seq(), 0L, Seq()))

    //memPoolManagerActor.expectMsg(GetProposedBlock)

    // TODO add assertions
  }

  "A ProposedBlockUpdated Message" should "be handled correctly" in new WithConsensusActor {

    consensusActor ! ProposedBlockUpdated(Block("hashPointer", 0L, "sig", Seq(), 0L, Seq()))

    // TODO add assertions
  }
  
  "A PeerProposedBlock Message" should "be handled correctly" in new WithConsensusActor {
    val peerProbe = TestProbe()

    consensusActor ! PeerProposedBlock(Block("hashPointer", 0L, "sig", Seq(), 0L, Seq()), peerProbe.ref)

    // TODO add assertions
  }

  "A CheckConsensusResult Message" should "be handled correctly" in new WithConsensusActor {
    consensusActor ! CheckConsensusResult()

    // TODO add assertions
  }

}
