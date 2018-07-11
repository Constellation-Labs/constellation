package org.constellation.consensus

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.testkit.{TestActor, TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.Data
import org.constellation.consensus.Consensus._
import org.constellation.p2p.{RegisterNextActor, UDPMessage, UDPSend}
import org.constellation.util.TestNode
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.collection.immutable.{HashMap, Map}
import org.constellation.Fixtures._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema.GetPeersID
import org.constellation.primitives.Schema._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor

class ConsensusTest extends TestKit(ActorSystem("ConsensusTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  trait WithConsensusActor {
    val udpActor = TestProbe()
    implicit val keyPair: KeyPair = KeyUtils.makeKeyPair()

    implicit val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)

    val consensusActor: ActorRef =
      system.actorOf(Props(
        new Consensus(keyPair, udpActor.ref)(timeout))
      )
  }

  "isFacilitator" should "return correctly if the actor is a facilitator" in {

    val isFacilitator = Consensus.isFacilitator(idSet4, id1)

    assert(isFacilitator)

    val isNotFacilitator = Consensus.isFacilitator(idSet4, id5)

    assert(!isNotFacilitator)
  }

  "the PerformConsensusRound" should "initialize and complete correctly in the CONFLICT scenario" ignore new WithConsensusActor {
    import constellation._

    val node2 = TestNode()
    val node3 = TestNode()
    val node4 = TestNode()

    val tx1 = createTransactionSafe(keyPair.getPublic.address, node2.configKeyPair.getPublic.address, 33L, keyPair)


    val tx2 = createTransactionSafe(node2.configKeyPair.getPublic.address, node4.configKeyPair.getPublic.address, 14L, node2.configKeyPair)

    val facilitators = Set(Id(keyPair.getPublic.encoded), Id(node2.configKeyPair.getPublic.encoded),
      Id(node3.configKeyPair.getPublic.encoded), Id(node4.configKeyPair.getPublic.encoded))

    val vote = Vote(VoteData(Seq(tx1), Seq(tx2)).signed()(keyPair = keyPair))
    val roundHash: RoundHash[Conflict] = RoundHash[Conflict](vote.vote.data.voteRoundHash)
    val bundle = Bundle(BundleData(vote.vote.data.accept).signed()(keyPair = keyPair))

    val callback = (result: ConsensusRoundResult[_ <: CC]) => {
      assert(result.bundle.bundleData.data.bundles == bundle.bundleData.data.bundles)
      assert(result.roundHash == roundHash)
      ()
    }

    consensusActor ! InitializeConsensusRound(facilitators, roundHash, callback, ConflictVote(vote))

    udpActor.expectMsg(UDPSend(StartConsensusRound(Id(keyPair.getPublic.encoded), ConflictVote(vote), roundHash), node2.udpAddress))

    udpActor.expectMsg(UDPSend(StartConsensusRound(Id(keyPair.getPublic.encoded), ConflictVote(vote), roundHash), node3.udpAddress))

    udpActor.expectMsg(UDPSend(StartConsensusRound(Id(keyPair.getPublic.encoded), ConflictVote(vote), roundHash), node4.udpAddress))

    consensusActor ! ConsensusVote(Id(node2.configKeyPair.getPublic.encoded), ConflictVote(vote), roundHash)

    consensusActor ! ConsensusVote(Id(node3.configKeyPair.getPublic.encoded), ConflictVote(vote), roundHash)

    consensusActor ! ConsensusVote(Id(node4.configKeyPair.getPublic.encoded), ConflictVote(vote), roundHash)

    // TODO: make more robust after update to bundles
    udpActor.expectMsgPF() {
      case UDPSend(message: ConsensusProposal[_], id: Id) => {
        assert(id == Id(node2.configKeyPair.getPublic.encoded))
       // assert(message.data == ConflictProposal(bundle))
        assert(message.roundHash == roundHash)
      }
    }

    udpActor.expectMsgPF() {
      case UDPSend(message: ConsensusProposal[_], id: Id) => {
        assert(id == Id(node3.configKeyPair.getPublic.encoded))
       // assert(message.data == ConflictProposal(bundle))
        assert(message.roundHash == roundHash)
      }
    }

    udpActor.expectMsgPF() {
      case UDPSend(message: ConsensusProposal[Conflict], id: Id) => {
        assert(id == Id(node4.configKeyPair.getPublic.encoded))
      //  assert(message.data == ConflictProposal(bundle))
        assert(message.roundHash == roundHash)
      }
    }

    consensusActor ! ConsensusProposal(Id(node2.configKeyPair.getPublic.encoded), ConflictProposal(bundle), roundHash)

    consensusActor ! ConsensusProposal(Id(node3.configKeyPair.getPublic.encoded), ConflictProposal(bundle), roundHash)

    consensusActor ! ConsensusProposal(Id(node4.configKeyPair.getPublic.encoded), ConflictProposal(bundle), roundHash)

    val consensusBundle = Bundle(BundleData(vote.vote.data.accept).signed())

  }

  "the PerformConsensusRound" should "initialize and complete correctly in the CHECKPOINT scenario" ignore new WithConsensusActor {
    import constellation._

    val node2 = TestNode()
    val node3 = TestNode()
    val node4 = TestNode()

    val tx1 = createTransactionSafe(keyPair.getPublic.address, node2.configKeyPair.getPublic.address, 33L, keyPair)


    val tx2 = createTransactionSafe(node2.configKeyPair.getPublic.address, node4.configKeyPair.getPublic.address, 14L, node2.configKeyPair)

    val facilitators = Set(Id(keyPair.getPublic.encoded), Id(node2.configKeyPair.getPublic.encoded),
      Id(node3.configKeyPair.getPublic.encoded), Id(node4.configKeyPair.getPublic.encoded))

    val vote = Vote(VoteData(Seq(tx1), Seq(tx2)).signed()(keyPair = keyPair))
    val roundHash: RoundHash[Checkpoint] = RoundHash[Checkpoint](vote.vote.data.voteRoundHash)
    val bundle = Bundle(BundleData(vote.vote.data.accept).signed()(keyPair = keyPair))

    val callback = (result: ConsensusRoundResult[_ <: CC]) => {
      assert(result.bundle.bundleData.data.bundles == bundle.bundleData.data.bundles)
      assert(result.roundHash == roundHash)
      ()
    }

    consensusActor ! InitializeConsensusRound(facilitators, roundHash, callback, CheckpointVote(bundle))

    udpActor.expectMsg(UDPSend(StartConsensusRound(Id(keyPair.getPublic.encoded), CheckpointVote(bundle), roundHash), node2.udpAddress))

    udpActor.expectMsg(UDPSend(StartConsensusRound(Id(keyPair.getPublic.encoded), CheckpointVote(bundle), roundHash), node3.udpAddress))

    udpActor.expectMsg(UDPSend(StartConsensusRound(Id(keyPair.getPublic.encoded), CheckpointVote(bundle), roundHash), node4.udpAddress))

    consensusActor ! ConsensusVote(Id(node2.configKeyPair.getPublic.encoded), CheckpointVote(bundle), roundHash)

    consensusActor ! ConsensusVote(Id(node3.configKeyPair.getPublic.encoded), CheckpointVote(bundle), roundHash)

    consensusActor ! ConsensusVote(Id(node4.configKeyPair.getPublic.encoded), CheckpointVote(bundle), roundHash)

    // TODO: make more robust after update to bundles
    udpActor.expectMsgPF() {
      case UDPSend(message: ConsensusProposal[Checkpoint], id: Id) => {
        assert(id == Id(node2.configKeyPair.getPublic.encoded))
        // assert(message.data == ConflictProposal(bundle))
        assert(message.roundHash == roundHash)
      }
    }

    udpActor.expectMsgPF() {
      case UDPSend(message: ConsensusProposal[Checkpoint], id: Id) => {
        assert(id == Id(node3.configKeyPair.getPublic.encoded))
        // assert(message.data == ConflictProposal(bundle))
        assert(message.roundHash == roundHash)
      }
    }

    udpActor.expectMsgPF() {
      case UDPSend(message: ConsensusProposal[Checkpoint], id: Id) => {
        assert(id == Id(node4.configKeyPair.getPublic.encoded))
        //  assert(message.data == ConflictProposal(bundle))
        assert(message.roundHash == roundHash)
      }
    }

    consensusActor ! ConsensusProposal(Id(node2.configKeyPair.getPublic.encoded), CheckpointProposal(bundle), roundHash)

    consensusActor ! ConsensusProposal(Id(node3.configKeyPair.getPublic.encoded), CheckpointProposal(bundle), roundHash)

    consensusActor ! ConsensusProposal(Id(node4.configKeyPair.getPublic.encoded), CheckpointProposal(bundle), roundHash)

  }

}
