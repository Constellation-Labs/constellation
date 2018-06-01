package org.constellation.consensus

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.testkit.{TestActor, TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.consensus.Consensus._
import org.constellation.p2p.PeerToPeer.{GetPeers, Id, Peers}
import org.constellation.p2p.{RegisterNextActor, UDPMessage, UDPSendToID}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.ChainStateManager.{AddBlock, CreateBlockProposal}
import org.constellation.utils.TestNode
import org.constellation.wallet.KeyUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.collection.immutable.{HashMap, Map}
import org.constellation.Fixtures._
import org.constellation.primitives.Schema._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor

class ConsensusTest extends TestKit(ActorSystem("ConsensusTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  private val address1: InetSocketAddress = constellation.addressToSocket("localhost:16181")
  private val address2: InetSocketAddress = constellation.addressToSocket("localhost:16182")
  private val address3: InetSocketAddress = constellation.addressToSocket("localhost:16183")
  private val address4: InetSocketAddress = constellation.addressToSocket("localhost:16184")
  private val address5: InetSocketAddress = constellation.addressToSocket("localhost:16185")

  implicit val materializer = ActorMaterializer()
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

  "the PerformConsensusRound" should "complete and respond with a consensus result" in new WithConsensusActor {
    import constellation._

    val node2 = TestNode()
    val node3 = TestNode()
    val node4 = TestNode()

    val tx1 = TX(TXData(Seq(keyPair.getPublic), node2.keyPair.getPublic, 33L).signed())

    val tx2 = TX(TXData(Seq(node2.keyPair.getPublic), node4.keyPair.getPublic, 14L).signed()(keyPair = node2.keyPair))

    val peerMemPools = HashMap(0L -> HashMap(
      Id(keyPair.getPublic) -> Seq(transaction1, transaction2),
      Id(node2.keyPair.getPublic) -> Seq(transaction1),
      Id(node3.keyPair.getPublic) -> Seq(transaction1, transaction2),
      Id(node4.keyPair.getPublic) -> Seq(transaction2)))

    val proposedBlock = Block("sig", 1, "", Set(Id(keyPair.getPublic), Id(node2.keyPair.getPublic),
      Id(node3.keyPair.getPublic), Id(node4.keyPair.getPublic)), 0, Seq(transaction1, transaction2))

    val facilitators = Set(Id(keyPair.getPublic), Id(node2.keyPair.getPublic),
      Id(node3.keyPair.getPublic), Id(node4.keyPair.getPublic))

    val replyTo = TestProbe()

    val vote = Vote(VoteData(Seq(tx1), Seq(tx2)).signed()(keyPair = keyPair))
    val round = RoundHash(vote.vote.data.voteRoundHash)
    val gossipHistory = Seq(Gossip(tx1.signed()))
    val bundle = Bundle(BundleData(vote.vote.data.accept).signed()(keyPair = keyPair))
    val roundHash = "testHash"

    consensusActor ! InitializeConsensusRound(facilitators, RoundHash(roundHash), replyTo.ref)

    /*
    udpActor.expectMsg(UDPSendToID(StartConsensusRound(Id(keyPair.getPublic), vote), Id(node2.keyPair.getPublic)))

    udpActor.expectMsg(UDPSendToID(StartConsensusRound(Id(keyPair.getPublic), vote), Id(node3.keyPair.getPublic)))

    udpActor.expectMsg(UDPSendToID(StartConsensusRound(Id(keyPair.getPublic), vote), Id(node4.keyPair.getPublic)))
    */

    consensusActor ! ConsensusVote(Id(node2.keyPair.getPublic), ConflictVote(vote), round)

    consensusActor ! ConsensusVote(Id(node3.keyPair.getPublic), ConflictVote(vote), round)

    consensusActor ! ConsensusVote(Id(node4.keyPair.getPublic), ConflictVote(vote), round)

    // TODO: make more robust after update to bundles
    udpActor.expectMsgPF() {
      case UDPSendToID(message: ConsensusProposal[Conflict], id: Id) => {
        assert(id == Id(node2.keyPair.getPublic))
        assert(message.data == bundle.bundleData.data.bundles)
        assert(message.roundHash == round)
      }
    }

    udpActor.expectMsgPF() {
      case UDPSendToID(message: ConsensusProposal[Conflict], id: Id) => {
        assert(id == Id(node3.keyPair.getPublic))
        assert(message.data == bundle.bundleData.data.bundles)
        assert(message.roundHash == round)
      }
    }

    udpActor.expectMsgPF() {
      case UDPSendToID(message: ConsensusProposal[Conflict], id: Id) => {
        assert(id == Id(node4.keyPair.getPublic))
        assert(message.data == bundle.bundleData.data.bundles)
        assert(message.roundHash == round)
      }
    }

    consensusActor ! ConsensusProposal(Id(node2.keyPair.getPublic), ConflictProposal(bundle), round)

    consensusActor ! ConsensusProposal(Id(node3.keyPair.getPublic), ConflictProposal(bundle), round)

    consensusActor ! ConsensusProposal(Id(node4.keyPair.getPublic), ConflictProposal(bundle), round)

    val consensusBundle = Bundle(BundleData(vote.vote.data.accept).signed())

    replyTo.expectMsgPF() {
      case ConsensusRoundResult(bundle: Bundle, roundHash: RoundHash[Conflict]) => {
        assert(bundle.bundleData.data.bundles == bundle.bundleData.data.bundles)
        assert(roundHash == round)
      }
    }

  }

}
