package org.constellation.consensus

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.constellation.consensus.Node.StartNewBlockCreationRound
import org.constellation.consensus.Round.{BroadcastTransactionProposal, BroadcastUnionBlockProposal, TransactionsProposal, UnionBlockProposal}
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema.{Id, SignedObservationEdge}
import org.constellation.primitives.{PeerData, Transaction}
import org.constellation.util.{APIClient, HashSignature, Metrics, SignatureBatch}
import org.constellation.{DAO, Fixtures, PeerMetadata}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global

class NodeConsensusTest extends TestKit(ActorSystem("test"))
  with FunSpecLike with Matchers with BeforeAndAfter with ImplicitSender with MockFactory {

  private implicit var fakeDao: DAO = _

  private var nodeA: TestActorRef[Node] = _
  private var nodeB: TestActorRef[Node] = _
  private var nodeC: TestActorRef[Node] = _

  private var remoteSenderProbe: TestProbe = _

  val sampleFacilitatorId = FacilitatorId(Id("facilitator-id-1"))

  private var sampleTransactions: Seq[Transaction] = _
  val samplePeer = PeerData(PeerMetadata("localhost", 0, 0, Id("1")), APIClient.apply(port = 9999))
  val sampleFacilitators: (Seq[SignedObservationEdge], Map[Id, PeerData]) = {
    (Seq(SignedObservationEdge(SignatureBatch("1", Seq(HashSignature("1", Id("1")))))),
     Map(Id("1") -> samplePeer))
  }

  before {
    fakeDao = stub[DAO]
    (fakeDao.id _).when().returns(Fixtures.id)
    fakeDao.keyPair = KeyUtils.makeKeyPair()
    fakeDao.metrics = new Metrics()
    val peerProbe = TestProbe.apply("peerManager")
    fakeDao.peerManager = peerProbe.ref

    sampleTransactions = Seq(
      Fixtures.dummyTx(fakeDao),
      Fixtures.dummyTx(fakeDao)
    )

    (fakeDao.readyPeers _)
      .when()
      .returns(Map(Fixtures.id1 -> samplePeer, Fixtures.id2 -> samplePeer))
    (fakeDao.pullTips _).when(*).returns(Some(sampleFacilitators))
    (fakeDao.pullTransactions _).when(*).returns(Some(sampleTransactions))
    remoteSenderProbe = TestProbe("remote-sender-probe")

    nodeA = TestActorRef(new Node(remoteSenderProbe.ref))

  }

  describe("Node on new block round") {
    it("should broadcast block building round to facilitators") {
      nodeA ! StartNewBlockCreationRound

      val proposal = remoteSenderProbe.expectMsgClass(classOf[BroadcastTransactionProposal])
      proposal.transactionsProposal.facilitatorId shouldBe FacilitatorId(fakeDao.id)
      proposal.peers.toSeq shouldBe Seq(samplePeer, samplePeer)
      proposal.transactionsProposal.transactions shouldBe sampleTransactions
    }
//    it("should broadcast no message to facilitators when there are no transactions") {
//      fakeDao = stub[DAO]
//      nodeActor = TestActorRef(new Node(remoteSenderProbe.ref)(fakeDao))
//
//      (fakeDao.pullTransactions _).when(*).returns(None)
//
//      nodeActor ! StartBlockCreationRound
//      remoteSenderProbe.expectNoMessage()
//    }

    it("should broadcast union transactions") {
      nodeA ! StartNewBlockCreationRound
      val roundId = remoteSenderProbe.expectMsgClass(classOf[BroadcastTransactionProposal])
        .transactionsProposal.roundId

      nodeA ! TransactionsProposal(roundId,
                                   FacilitatorId(fakeDao.id),
                                   sampleTransactions)
      val extraTransactions = Seq(Fixtures.dummyTx(fakeDao, 10, Fixtures.id1))

      nodeA ! TransactionsProposal(
        roundId,
        FacilitatorId(fakeDao.id),
        sampleTransactions ++ extraTransactions
      )
      nodeA ! TransactionsProposal(
        RoundId("other-round"),
        FacilitatorId(fakeDao.id),
        sampleTransactions ++ Seq(Fixtures.dummyTx(fakeDao, 12, Fixtures.id2))
      )
      val unionBlock = remoteSenderProbe.expectMsgClass(classOf[BroadcastUnionBlockProposal])
      unionBlock.proposal.checkpointBlock.transactions shouldBe sampleTransactions ++ extraTransactions

//      Facilitators accept the block with greatest number of signatures (each facilitator counts signatures on his own)
    }
  }

}
