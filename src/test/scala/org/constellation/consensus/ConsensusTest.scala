package org.constellation.consensus

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActor, TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.consensus.Consensus._
import org.constellation.p2p.PeerToPeer.GetPeerActorRefs
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.ChainStateManager.{AddBlock, BlockAddedToChain, CreateBlockProposal}
import org.constellation.wallet.KeyUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.duration._
import scala.collection.immutable.HashMap
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
          new Consensus(memPoolManagerActor.ref, chainStateManagerActor.ref, keyPair)(timeout)))
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

  // TODO: modify to use threshold
  "getConsensusBlock" should "return a consensus block if the proposals are in sync" in {

    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()
    val node3KeyPair = KeyUtils.makeKeyPair()
    val node4KeyPair = KeyUtils.makeKeyPair()

    val transaction1 =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val transaction2 =
      Transaction.senderSign(Transaction(0L, node2KeyPair.getPublic, node4KeyPair.getPublic, 14L), node2KeyPair.getPrivate)

    val transaction3 =
      Transaction.senderSign(Transaction(0L, node4KeyPair.getPublic, node1KeyPair.getPublic, 2L), node4KeyPair.getPrivate)

    val transaction4 =
      Transaction.senderSign(Transaction(0L, node3KeyPair.getPublic, node2KeyPair.getPublic, 20L), node3KeyPair.getPrivate)

    val node1Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node2Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node3Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node4Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))

    val peerBlockProposals = HashMap(0L -> HashMap(node1.ref -> node1Block, node2.ref -> node2Block, node3.ref -> node3Block, node4.ref -> node4Block))

    val currentFacilitators = Set(node1.ref, node2.ref, node3.ref, node4.ref)

    val consensusBlock: Option[Block] =
      Consensus.getConsensusBlock(peerBlockProposals, currentFacilitators, 0L)

    assert(consensusBlock.isDefined)

    assert(consensusBlock.get == node1Block)
  }

  "getConsensusBlock" should "not return a consensus block if the proposals are not in sync" in {

    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()
    val node3KeyPair = KeyUtils.makeKeyPair()
    val node4KeyPair = KeyUtils.makeKeyPair()

    val transaction =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val node1Block = Block("sig", 0, "", Set(), 0, Seq())
    val node2Block = Block("sig", 0, "", Set(), 0, Seq(transaction))
    val node3Block = Block("sig", 0, "", Set(), 0, Seq())
    val node4Block = Block("sig", 0, "", Set(), 0, Seq())

    val peerBlockProposals = HashMap(0L -> HashMap(node1.ref -> node1Block, node2.ref -> node2Block, node3.ref -> node3Block, node4.ref -> node4Block))

    val currentFacilitators = Set(node1.ref, node2.ref, node3.ref, node4.ref)

    val consensusBlock: Option[Block] =
      Consensus.getConsensusBlock(peerBlockProposals, currentFacilitators, 0L)

    assert(consensusBlock.isEmpty)
  }

  "the handleBlockAddedToChain" should "return the correct state and trigger a new consensus round" in new WithConsensusActor {

    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()
    val node5 = TestProbe()

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()

    val proposedBlock = Block("sig", 0, "", Set(), 1L, Seq())
    val prevBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node4.ref), 0L, Seq())
    val latestBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node5.ref), 1L, Seq())

    val transaction =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    // Test with consensus enabled and we are a facilitator
    val consensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      Some(proposedBlock),
      Some(prevBlock),
      prevBlock.clusterParticipants,
      HashMap(1L -> HashMap(node1.ref -> Seq(transaction))),
      HashMap(1L -> HashMap(node1.ref -> proposedBlock)))

    val memPoolManager = TestProbe()

    val updatedConsensusState =
      Consensus.handleBlockAddedToChain(consensusRoundState, latestBlock, memPoolManager.ref, node1.ref)

    val expectedConsensusState = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(latestBlock),
      latestBlock.clusterParticipants,
      HashMap(),
      HashMap())

    assert(updatedConsensusState == expectedConsensusState)

    memPoolManager.expectMsg(GetMemPool(node1.ref, latestBlock.round + 1))

    // Test with consensus enabled and we are not a facilitator
    val consensusRoundState2 = ConsensusRoundState(
      Some(node1.ref),
      true,
      Some(proposedBlock),
      Some(prevBlock),
      prevBlock.clusterParticipants,
      HashMap(0L -> HashMap(node1.ref -> Seq(transaction))),
      HashMap(0L -> HashMap(node1.ref -> proposedBlock)))

    val latestBlock2 = Block("sig", 0, "", Set(node2.ref, node3.ref, node5.ref), 1L, Seq())

    val updatedConsensusState2 =
      Consensus.handleBlockAddedToChain(consensusRoundState2, latestBlock2, memPoolManager.ref, node1.ref)

    val expectedConsensusState2 = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(latestBlock2),
      latestBlock2.clusterParticipants,
      HashMap(),
      HashMap())

    assert(updatedConsensusState2 == expectedConsensusState2)

    memPoolManager.expectNoMsg()

    // Test with consensus disabled, ensure we are not filtering current round caches
    val consensusRoundState3 = ConsensusRoundState(
      Some(node1.ref),
      false,
      Some(proposedBlock),
      Some(prevBlock),
      prevBlock.clusterParticipants,
      HashMap(1L -> HashMap(node1.ref -> Seq(transaction)), 2L -> HashMap(node2.ref -> Seq(transaction))),
      HashMap(1L -> HashMap(node1.ref -> proposedBlock), 2L -> HashMap(node2.ref -> proposedBlock)))

    val latestBlock3 = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node5.ref), 1L, Seq())

    val updatedConsensusState3 =
      Consensus.handleBlockAddedToChain(consensusRoundState3, latestBlock3, memPoolManager.ref, node1.ref)

    val expectedConsensusState3 = ConsensusRoundState(
      Some(node1.ref),
      false,
      None,
      Some(latestBlock3),
      latestBlock3.clusterParticipants,
      HashMap(2L -> HashMap(node2.ref -> Seq(transaction))),
      HashMap(2L -> HashMap(node2.ref -> proposedBlock)))

    assert(updatedConsensusState3 == expectedConsensusState3)

    memPoolManager.expectNoMsg()
  }

  "the generateGenesisBlock method" should "work correctly" in new WithConsensusActor {
    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()
    val node5 = TestProbe()

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()

    val proposedBlock = Block("sig", 0, "", Set(), 1L, Seq())
    val prevBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node4.ref), 0L, Seq())
    val latestBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node5.ref), 1L, Seq())

    val transaction =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val consensusRoundState = ConsensusRoundState(
      None,
      false,
      None,
      None,
      Set(),
      HashMap(0L -> HashMap(node2.ref -> Seq(transaction))),
      HashMap(0L -> HashMap(node2.ref -> proposedBlock)))

    val chainStateManager = TestProbe()

    val requestActor = TestProbe()

    val testConsensusActor = TestProbe()

    node1.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
        sender ! Set(node2.ref, node3.ref, node4.ref, node5.ref)
        TestActor.KeepRunning
      }
    })

    val updatedConsensusState = Consensus.generateGenesisBlock(node1.ref, consensusRoundState, chainStateManager.ref,
      requestActor.ref, testConsensusActor.ref)

    node1.expectMsg(GetPeerActorRefs)

    val expectedConsensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      false,
      None,
      None,
      Set(),
      HashMap(0L -> HashMap(node2.ref -> Seq(transaction))),
      HashMap(0L -> HashMap(node2.ref -> proposedBlock)))

    assert(updatedConsensusState == expectedConsensusRoundState)

    val genesisBlock = Block("tempGenesisParentHash", 0, "tempSig",
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref), 0, Seq())

    chainStateManager.expectMsg(AddBlock(genesisBlock, testConsensusActor.ref))

    requestActor.expectMsg(genesisBlock)
  }

  "the enableConsensus method" should "work correctly" in new WithConsensusActor {
    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()
    val node5 = TestProbe()

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()

    val proposedBlock = Block("sig", 0, "", Set(), 1L, Seq())
    val prevBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node4.ref), 0L, Seq())
    val latestBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node5.ref), 1L, Seq())

    val transaction =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    // verify it triggers when we are part of this consensus round
    val consensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      false,
      None,
      None,
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(0L -> HashMap(node2.ref -> Seq(transaction))),
      HashMap(0L -> HashMap(node2.ref -> proposedBlock)))

    val memPoolManager = TestProbe()
    val testConsensusActor = TestProbe()

    val updatedConsensusState = Consensus.enableConsensus(consensusRoundState, memPoolManager.ref, testConsensusActor.ref)

    memPoolManager.expectMsg(GetMemPool(testConsensusActor.ref, 0L))

    val expectedConsensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      None,
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(0L -> HashMap(node2.ref -> Seq(transaction))),
      HashMap(0L -> HashMap(node2.ref -> proposedBlock)))

    assert(updatedConsensusState == expectedConsensusRoundState)

    // verify it does not trigger when we are not part of this consensus round
    val consensusRoundState2 = ConsensusRoundState(
      Some(node1.ref),
      false,
      None,
      None,
      Set(node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(0L -> HashMap(node2.ref -> Seq(transaction))),
      HashMap(0L -> HashMap(node2.ref -> proposedBlock)))

    val updatedConsensusState2 = Consensus.enableConsensus(consensusRoundState2, memPoolManager.ref, testConsensusActor.ref)

    memPoolManager.expectNoMsg()

    val expectedConsensusRoundState2 = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      None,
      Set(node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(0L -> HashMap(node2.ref -> Seq(transaction))),
      HashMap(0L -> HashMap(node2.ref -> proposedBlock)))

    assert(updatedConsensusState2 == expectedConsensusRoundState2)
  }

  "the handleProposedBlockUpdated method" should "work correctly" in new WithConsensusActor {
    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()
    val node5 = TestProbe()

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()

    val proposedBlock = Block("sig", 0, "", Set(), 1L, Seq())
    val prevBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref), 0L, Seq())

    val transaction =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val consensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(prevBlock),
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(0L -> HashMap(node2.ref -> Seq(transaction))),
      HashMap(0L -> HashMap(node2.ref -> proposedBlock)))

    val memPoolManager = TestProbe()
    val testConsensusActor = TestProbe()

    val updatedConsensusState = Consensus.handleProposedBlockUpdated(consensusRoundState, proposedBlock)

    node1.expectMsg(PeerProposedBlock(proposedBlock, node1.ref))
    node2.expectMsg(PeerProposedBlock(proposedBlock, node1.ref))
    node3.expectMsg(PeerProposedBlock(proposedBlock, node1.ref))
    node4.expectMsg(PeerProposedBlock(proposedBlock, node1.ref))
    node5.expectMsg(PeerProposedBlock(proposedBlock, node1.ref))

    val expectedConsensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      Some(proposedBlock),
      Some(prevBlock),
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(0L -> HashMap(node2.ref -> Seq(transaction))),
      HashMap(0L -> HashMap(node2.ref -> proposedBlock)))

    assert(updatedConsensusState == expectedConsensusRoundState)
  }

  "the checkConsensusResult method" should "add the block if in consensus" in new WithConsensusActor {
    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()
    val node3KeyPair = KeyUtils.makeKeyPair()
    val node4KeyPair = KeyUtils.makeKeyPair()

    val transaction1 =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val transaction2 =
      Transaction.senderSign(Transaction(0L, node2KeyPair.getPublic, node4KeyPair.getPublic, 14L), node2KeyPair.getPrivate)

    val transaction3 =
      Transaction.senderSign(Transaction(0L, node4KeyPair.getPublic, node1KeyPair.getPublic, 2L), node4KeyPair.getPrivate)

    val transaction4 =
      Transaction.senderSign(Transaction(0L, node3KeyPair.getPublic, node2KeyPair.getPublic, 20L), node3KeyPair.getPrivate)

    val node1Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node2Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node3Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node4Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))

    val peerBlockProposals = HashMap(0L -> HashMap(node1.ref -> node1Block, node2.ref -> node2Block, node3.ref -> node3Block, node4.ref -> node4Block))

    val currentFacilitators = Set(node1.ref, node2.ref, node3.ref, node4.ref)

    val prevBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node4.ref), 0L, Seq())

    val consensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(prevBlock),
      currentFacilitators,
      HashMap(1L -> HashMap(node2.ref -> Seq(transaction1))),
      peerBlockProposals)

    val chainStateManager = TestProbe()

    val testConsensusActor = TestProbe()

    Consensus.checkConsensusResult(consensusRoundState, 0L, chainStateManager.ref, testConsensusActor.ref)

    chainStateManager.expectMsg(AddBlock(node1Block, testConsensusActor.ref))
  }

  "the checkConsensusResult method" should "not add the block if not in consensus" in new WithConsensusActor {
    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()
    val node3KeyPair = KeyUtils.makeKeyPair()
    val node4KeyPair = KeyUtils.makeKeyPair()

    val transaction1 =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val transaction2 =
      Transaction.senderSign(Transaction(0L, node2KeyPair.getPublic, node4KeyPair.getPublic, 14L), node2KeyPair.getPrivate)

    val transaction3 =
      Transaction.senderSign(Transaction(0L, node4KeyPair.getPublic, node1KeyPair.getPublic, 2L), node4KeyPair.getPrivate)

    val transaction4 =
      Transaction.senderSign(Transaction(0L, node3KeyPair.getPublic, node2KeyPair.getPublic, 20L), node3KeyPair.getPrivate)

    val node1Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3))
    val node2Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node3Block = Block("sig", 0, "", Set(node1.ref, node3.ref, node4.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node4Block = Block("sig", 0, "", Set(node1.ref, node3.ref), 0, Seq(transaction1, transaction2, transaction3, transaction4))

    val peerBlockProposals = HashMap(0L -> HashMap(node1.ref -> node1Block, node2.ref -> node2Block, node3.ref -> node3Block, node4.ref -> node4Block))

    val currentFacilitators = Set(node1.ref, node2.ref, node3.ref, node4.ref)

    val prevBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node4.ref), 0L, Seq())

    val consensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(prevBlock),
      currentFacilitators,
      HashMap(1L -> HashMap(node2.ref -> Seq(transaction1))),
      peerBlockProposals)

    val chainStateManager = TestProbe()

    val testConsensusActor = TestProbe()

    Consensus.checkConsensusResult(consensusRoundState, 0L, chainStateManager.ref, testConsensusActor.ref)

    chainStateManager.expectNoMsg()
  }

  "the handlePeerMemPoolUpdated method" should "work correctly" in new WithConsensusActor {
    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()
    val node5 = TestProbe()

    val proposedBlock = Block("sig", 0, "", Set(), 1L, Seq())
    val prevBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref), 0L, Seq())

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()
    val node3KeyPair = KeyUtils.makeKeyPair()
    val node4KeyPair = KeyUtils.makeKeyPair()

    val transaction1 =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val transaction2 =
      Transaction.senderSign(Transaction(0L, node2KeyPair.getPublic, node4KeyPair.getPublic, 14L), node2KeyPair.getPrivate)

    val transaction3 =
      Transaction.senderSign(Transaction(0L, node4KeyPair.getPublic, node1KeyPair.getPublic, 2L), node4KeyPair.getPrivate)

    val transaction4 =
      Transaction.senderSign(Transaction(0L, node3KeyPair.getPublic, node2KeyPair.getPublic, 20L), node3KeyPair.getPrivate)

    // verify that when all of this rounds mem pools are available we create a block proposal
    val consensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(prevBlock),
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(0L -> HashMap(
        node1.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node2.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node3.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node4.ref -> Seq(transaction1, transaction2, transaction3, transaction4))),
      HashMap())

    val chainStateManager = TestProbe()
    val testConsensusActor = TestProbe()

    val updatedConsensusState = Consensus.handlePeerMemPoolUpdated(consensusRoundState, 0L, node5.ref,
      Seq(transaction1, transaction2, transaction3, transaction4), chainStateManager.ref, testConsensusActor.ref)

    chainStateManager.expectMsg(CreateBlockProposal(HashMap(
      node1.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
      node2.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
      node3.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
      node4.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
      node5.ref -> Seq(transaction1, transaction2, transaction3, transaction4)), 0L, testConsensusActor.ref))

    val expectedConsensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(prevBlock),
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(0L -> HashMap(
        node1.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node2.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node3.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node4.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node5.ref -> Seq(transaction1, transaction2, transaction3, transaction4))),
      HashMap())

    assert(updatedConsensusState == expectedConsensusRoundState)

    // verify that when there are not all of the mem pools it does not create a block proposal
    val consensusRoundState2 = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(prevBlock),
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(0L -> HashMap(
        node1.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node3.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node4.ref -> Seq(transaction1, transaction2, transaction3, transaction4))),
      HashMap())

    val updatedConsensusState2 = Consensus.handlePeerMemPoolUpdated(consensusRoundState2, 0L, node5.ref,
      Seq(transaction1, transaction2, transaction3, transaction4), chainStateManager.ref, testConsensusActor.ref)

    chainStateManager.expectNoMsg()

    val expectedConsensusRoundState2 = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(prevBlock),
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(0L -> HashMap(
        node1.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node3.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node4.ref -> Seq(transaction1, transaction2, transaction3, transaction4),
        node5.ref -> Seq(transaction1, transaction2, transaction3, transaction4))),
      HashMap())

    assert(updatedConsensusState2 == expectedConsensusRoundState2)
  }

  "the handlePeerProposedBlock method" should "work correctly" in new WithConsensusActor {
    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()
    val node5 = TestProbe()

    val proposedBlock = Block("sig", 0, "", Set(), 1L, Seq())
    val prevBlock = Block("sig", 0, "", Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref), 0L, Seq())

    // verify that when all of this rounds mem pools are available we create a block proposal
    val consensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(prevBlock),
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(),
      HashMap(1L -> HashMap(node2.ref -> proposedBlock)))

    val chainStateManager = TestProbe()
    val testConsensusActor = TestProbe()

    val updatedConsensusState = Consensus.handlePeerProposedBlock(consensusRoundState, testConsensusActor.ref, proposedBlock, node1.ref)

    testConsensusActor.expectMsg(CheckConsensusResult(proposedBlock.round))

    val expectedConsensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      None,
      Some(prevBlock),
      Set(node1.ref, node2.ref, node3.ref, node4.ref, node5.ref),
      HashMap(),
      HashMap(1L -> HashMap(node2.ref -> proposedBlock, node1.ref -> proposedBlock)))

    assert(updatedConsensusState == expectedConsensusRoundState)
  }

}
