package org.constellation.consensus

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.consensus.Consensus._
import org.constellation.primitives.Block
import org.constellation.state.ChainStateManager.BlockAddedToChain
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
      val peerToPeerActor = TestProbe()
      val keyPair: KeyPair = KeyUtils.makeKeyPair()

     implicit val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)

      val consensusActor: ActorRef =
        system.actorOf(Props(
          new Consensus(memPoolManagerActor.ref, chainStateManagerActor.ref, peerToPeerActor.ref, keyPair)(timeout)))
  }

  "getFacilitators" should "give back the correct list of facilitators" in {
    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()

    val membersOfCluster = Set(node1.ref, node2.ref, node3.ref, node4.ref)

    val block = Block("hashPointer", 0L, "sig", membersOfCluster, 0L, Seq())

    val facilitators = Consensus.getFacilitators(block)

    // TODO: modify once we have subset filtering logic
    val expectedFacilitators = Set(node1.ref, node2.ref, node3.ref, node4.ref)

    assert(facilitators == expectedFacilitators)
  }

  "notifyFacilitatorsOfBlockProposal" should "not propose a block if it is currently not a facilitator" in {

    val self = TestProbe()
    val peer1 = TestProbe()

    val prevBlock = Block("hashPointer", 0L, "sig", Set(peer1.ref), 0L, Seq())

    val block = Block("hashPointer", 0L, "sig", Set(peer1.ref), 0L, Seq())

    val notify = Consensus.notifyFacilitatorsOfBlockProposal(prevBlock, block, self.ref)

    assert(!notify)
  }

  "notifyFacilitatorsOfBlockProposal" should "notify all of the correct facilitators with the correct block" in {

    val self = TestProbe()
    val peer1 = TestProbe()
    val peer2 = TestProbe()
    val peer3 = TestProbe()

    val prevBlock = Block("prevhashPointer", 0L, "prevsig", Set(peer1.ref, self.ref, peer2.ref, peer3.ref), 0L, Seq())
    val block = Block("hashPointer", 0L, "sig", Set(peer1.ref, self.ref, peer2.ref, peer3.ref), 0L, Seq())

    val notify = Consensus.notifyFacilitatorsOfBlockProposal(prevBlock, block, self.ref)

    val peerProposedBlock = PeerProposedBlock(block, self.ref)

    peer1.expectMsg(peerProposedBlock)
    peer2.expectMsg(peerProposedBlock)
    peer3.expectMsg(peerProposedBlock)
    self.expectMsg(peerProposedBlock)

    assert(notify)
  }

  "isFacilitator" should "return correctly if the actor is a facilitator" in {

    val self = TestProbe()
    val peer1 = TestProbe()
    val peer2 = TestProbe()

    val isFacilitator = Consensus.isFacilitator(Set(peer1.ref, self.ref, peer2.ref), self.ref)

    assert(isFacilitator)

    val isNotFacilitator = Consensus.isFacilitator(Set(peer1.ref, peer2.ref), self.ref)

    assert(!isNotFacilitator)
  }

  "getConsensusBlock" should "return the correct consensus block or not based on the state of consensus proposals" in {

    val testProbe = TestProbe()

    val consensusBlock: Option[Block] = Consensus.getConsensusBlock(mutable.HashMap(), Set())

    // TODO add assertions
  }

  "A BlockAddedToChain Message" should "be handled correctly" in new WithConsensusActor {

    consensusActor ! BlockAddedToChain(Block("hashPointer", 0L, "sig", Set(), 0L, Seq()))

    //memPoolManagerActor.expectMsg(GetProposedBlock)

    // TODO add assertions
  }

  "A ProposedBlockUpdated Message" should "be handled correctly" in new WithConsensusActor {

    consensusActor ! ProposedBlockUpdated(Block("hashPointer", 0L, "sig", Set(), 0L, Seq()))

    // TODO add assertions
  }

  "A PeerProposedBlock Message" should "be handled correctly" in new WithConsensusActor {
    val peerProbe = TestProbe()

    consensusActor ! PeerProposedBlock(Block("hashPointer", 0L, "sig", Set(), 0L, Seq()), peerProbe.ref)

    // TODO add assertions
  }

  "A CheckConsensusResult Message" should "be handled correctly" in new WithConsensusActor {
    consensusActor ! CheckConsensusResult()

    // TODO add assertions
  }

}
