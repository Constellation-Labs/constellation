package org.constellation.consensus

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActor, TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.consensus.Consensus._
import org.constellation.p2p.PeerToPeer.{GetPeers, Peers}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.ChainStateManager.{AddBlock, BlockAddedToChain}
import org.constellation.wallet.KeyUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.duration._
import scala.collection.immutable.HashMap
import scala.collection.mutable

class ConsensusTest extends TestKit(ActorSystem("ConsensusTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
  private val address1: InetSocketAddress = constellation.addressToSocket("localhost:16181")
  private val address2: InetSocketAddress = constellation.addressToSocket("localhost:16182")
  private val address3: InetSocketAddress = constellation.addressToSocket("localhost:16183")
  private val address4: InetSocketAddress = constellation.addressToSocket("localhost:16184")
  private val address5: InetSocketAddress = constellation.addressToSocket("localhost:16185")

  trait WithConsensusActor {
    val memPoolManagerActor = TestProbe()
    val chainStateManagerActor = TestProbe()
    val peerToPeerActor = TestProbe()
    val udpActor = TestProbe()
    val keyPair: KeyPair = KeyUtils.makeKeyPair()

    implicit val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)

    val consensusActor: ActorRef =
      system.actorOf(Props(
        new Consensus(
          memPoolManagerActor.ref, chainStateManagerActor.ref, keyPair, address1, udpActor.ref
        )(timeout))
      )
  }

  "getFacilitators" should "give back the correct list of facilitators" in {
    val node1 = TestProbe()
    val node2 = TestProbe()
    val node3 = TestProbe()
    val node4 = TestProbe()

    val membersOfCluster = Set(address1, address2, address3, address4)

    val block = Block("hashPointer", 0L, "sig", membersOfCluster, 0L, Seq())

    val facilitators = Consensus.getFacilitators(block)

    // TODO: modify once we have subset filtering logic
    val expectedFacilitators = Set(address1, address2, address3, address4)

    assert(facilitators == expectedFacilitators)
  }

  "notifyFacilitatorsOfBlockProposal" should "not propose a block if it is currently not a facilitator" in {

    val self = TestProbe()
    val peer1 = TestProbe()

    val prevBlock = Block("hashPointer", 0L, "sig", Set(address2), 0L, Seq())

    val block = Block("hashPointer", 0L, "sig", Set(address2), 0L, Seq())

    // TODO : Fix after UDP changes.. this function changed somewhat in that it relies on udp now
    // Abstract it properly into separate defs that don't require the actor -- later
    // val notify = Consensus.notifyFacilitatorsOfBlockProposal(prevBlock, block, address1)

   // assert(!notify)
  }

/*
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
*/

  "isFacilitator" should "return correctly if the actor is a facilitator" in {

    val self = TestProbe()
    val peer1 = TestProbe()
    val peer2 = TestProbe()

    val isFacilitator = Consensus.isFacilitator(Set(address1, address2, address3), address1)

    assert(isFacilitator)

    val isNotFacilitator = Consensus.isFacilitator(Set(address1, address2), address3)

    assert(!isNotFacilitator)
  }

  // TODO: modify to use threshold
  "getConsensusBlock" should "return a consensus block if the proposals are in sync" in {

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

    val node1Block = Block("sig", 0, "", Set(address1, address3), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node2Block = Block("sig", 0, "", Set(address1, address3), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node3Block = Block("sig", 0, "", Set(address1, address3), 0, Seq(transaction1, transaction2, transaction3, transaction4))
    val node4Block = Block("sig", 0, "", Set(address1, address3), 0, Seq(transaction1, transaction2, transaction3, transaction4))

    val peerBlockProposals = HashMap(
      0L -> HashMap(address1 -> node1Block, address2 -> node2Block, address3 -> node3Block, address4 -> node4Block)
    )

    val currentFacilitators = Set(address1, address2, address3, address4)

    val consensusBlock: Option[Block] =
      Consensus.getConsensusBlock(peerBlockProposals, currentFacilitators, 0L)

    assert(consensusBlock.isDefined)

    assert(consensusBlock.get == node1Block)
  }

  "getConsensusBlock" should "not return a consensus block if the proposals are not in sync" in {


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

    val peerBlockProposals = HashMap(
      0L -> HashMap(address1 -> node1Block, address2 -> node2Block, address3 -> node3Block, address4 -> node4Block)
    )

    val currentFacilitators = Set(address1, address2, address3, address4)

    val consensusBlock: Option[Block] =
      Consensus.getConsensusBlock(peerBlockProposals, currentFacilitators, 0L)

    assert(consensusBlock.isEmpty)
  }

  "the handleBlockAddedToChain" should "return the correct state and trigger a new consensus round" in new WithConsensusActor {

    val node1 = TestProbe()

    val node1KeyPair = KeyUtils.makeKeyPair()
    val node2KeyPair = KeyUtils.makeKeyPair()

    val proposedBlock = Block("sig", 0, "", Set(), 1L, Seq())
    val prevBlock = Block("sig", 0, "", Set(address1, address2, address3, address4), 0L, Seq())
    val latestBlock = Block("sig", 0, "", Set(address1, address2, address3, address5), 1L, Seq())

    val transaction =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    // Test with consensus enabled and we are a facilitator
    val consensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      true,
      Some(proposedBlock),
      Some(prevBlock),
      prevBlock.clusterParticipants,
      HashMap(1L -> HashMap(address1 -> Seq(transaction))),
      HashMap(1L -> HashMap(address1 -> proposedBlock)))

    val memPoolManager = TestProbe()

    val updatedConsensusState =
      Consensus.handleBlockAddedToChain(consensusRoundState, latestBlock, memPoolManager.ref, node1.ref, address1)

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
      HashMap(0L -> HashMap(address1 -> Seq(transaction))),
      HashMap(0L -> HashMap(address1 -> proposedBlock)))

    val latestBlock2 = Block("sig", 0, "", Set(address2, address3, address5), 1L, Seq())

    val updatedConsensusState2 =
      Consensus.handleBlockAddedToChain(consensusRoundState2, latestBlock2, memPoolManager.ref, node1.ref, address1)

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
      HashMap(1L -> HashMap(address1 -> Seq(transaction)), 2L -> HashMap(address2 -> Seq(transaction))),
      HashMap(1L -> HashMap(address1 -> proposedBlock), 2L -> HashMap(address2 -> proposedBlock)))

    val latestBlock3 = Block("sig", 0, "", Set(address1, address2, address3, address5), 1L, Seq())

    val updatedConsensusState3 =
      Consensus.handleBlockAddedToChain(consensusRoundState3, latestBlock3, memPoolManager.ref, node1.ref, address1)

    val expectedConsensusState3 = ConsensusRoundState(
      Some(node1.ref),
      false,
      None,
      Some(latestBlock3),
      latestBlock3.clusterParticipants,
      HashMap(2L -> HashMap(address2 -> Seq(transaction))),
      HashMap(2L -> HashMap(address2 -> proposedBlock)))

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
    val prevBlock = Block("sig", 0, "", Set(address1, address2, address3, address4), 0L, Seq())
    val latestBlock = Block("sig", 0, "", Set(address1, address2, address3, address5), 1L, Seq())

    val transaction =
      Transaction.senderSign(Transaction(0L, node1KeyPair.getPublic, node2KeyPair.getPublic, 33L), node1KeyPair.getPrivate)

    val consensusRoundState = ConsensusRoundState(
      None,
      false,
      None,
      None,
      Set(),
      HashMap(0L -> HashMap(address2 -> Seq(transaction))),
      HashMap(0L -> HashMap(address2 -> proposedBlock)))

    val chainStateManager = TestProbe()

    val requestActor = TestProbe()

    val testConsensusActor = TestProbe()

    node1.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
        sender ! Peers(Seq(address2, address3, address4, address5))
        TestActor.KeepRunning
      }
    })

    val updatedConsensusState = Consensus.generateGenesisBlock(node1.ref, consensusRoundState, chainStateManager.ref,
      requestActor.ref, testConsensusActor.ref, address1)

    node1.expectMsg(GetPeers)

    val expectedConsensusRoundState = ConsensusRoundState(
      Some(node1.ref),
      false,
      None,
      None,
      Set(),
      HashMap(0L -> HashMap(address2 -> Seq(transaction))),
      HashMap(0L -> HashMap(address2 -> proposedBlock)))

    assert(updatedConsensusState == expectedConsensusRoundState)

    val genesisBlock = Block("tempGenesisParentHash", 0, "tempSig",
      Set(address1, address2, address3, address4, address5), 0, Seq())

    chainStateManager.expectMsg(AddBlock(genesisBlock, testConsensusActor.ref))

    requestActor.expectMsg(genesisBlock)

  }

  "the enableConsensus method" should "work correctly" in new WithConsensusActor {
    // TODO add assertions
  }

  "the handleProposedBlockUpdated method" should "work correctly" in new WithConsensusActor {
    // TODO add assertions
  }

  // "the checkConsensusResult method" should "work correctly" in new WithConsensusActor {
  "A PeerProposedBlock Message" should "be handled correctly" in new WithConsensusActor {
    val peerProbe = TestProbe()

 //   consensusActor ! PeerProposedBlock(Block("hashPointer", 0L, "sig", Set(), 0L, Seq()), peerProbe.ref)

    // TODO add assertions
  }

  "the handlePeerMemPoolUpdated method" should "work correctly" in new WithConsensusActor {
    // TODO add assertions
  }

  "the handlePeerProposedBlock method" should "work correctly" in new WithConsensusActor {
    // TODO add assertions
  }

}
