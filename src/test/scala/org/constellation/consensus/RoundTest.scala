package org.constellation.consensus

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.constellation.consensus.Round._
import org.constellation.consensus.RoundManager.{BroadcastTransactionProposal, BroadcastUnionBlockProposal}
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema.{EdgeHashType, Id, SignedObservationEdge, TypedEdgeHash}
import org.constellation.primitives.{CheckpointBlock, PeerData, Transaction}
import org.constellation.util.{APIClient, HashSignature, Metrics, SignatureBatch}
import org.constellation.{DAO, Fixtures, NodeInitializationConfig, PeerMetadata}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers, OneInstancePerTest}

import scala.concurrent.ExecutionContext.Implicits.global

class RoundTest
    extends TestKit(ActorSystem("RoundTest"))
    with FunSpecLike
    with Matchers
    with BeforeAndAfter
    with ImplicitSender
    with MockFactory
    with OneInstancePerTest {

  private implicit var fakeDao: DAO = _

  private var roundProbe: TestActorRef[Round] = _

  val peerA =
    PeerData(PeerMetadata("localhost", 0, Id("peer-A")), APIClient.apply(port = 9999))
  val peerB = PeerData(peerA.peerMetadata.copy(id = Id("peer-B")), APIClient.apply(port = 9999))
  var roundData: RoundData = _
  val roundManagerActor = TestProbe("round-manager")

  var sampleTransactions: Seq[Transaction] = _
  val tips: Seq[SignedObservationEdge] = Seq(
    SignedObservationEdge(SignatureBatch("1", Seq(HashSignature("1", Id("1")))))
  )

  // TODO This throws a null pointer on nodeconfig, fix later
  def initBefore = {
    fakeDao = stub[DAO]
    (fakeDao.id _).when().returns(Fixtures.id)
    (fakeDao.nodeConfig _).when().returns(NodeInitializationConfig())
    fakeDao.keyPair = KeyUtils.makeKeyPair()
    fakeDao.metrics = new Metrics()
    val peerProbe = TestProbe.apply("peerManager")
    fakeDao.peerManager = peerProbe.ref

    sampleTransactions = Seq(
      Fixtures.dummyTx(fakeDao),
      Fixtures.dummyTx(fakeDao)
    )
    roundData = RoundData(RoundId("round-1"),
                          Set(peerA, peerB),
                          FacilitatorId(fakeDao.id),
                          sampleTransactions,
                          tips,
                          Seq.empty)
    (fakeDao.pullTransactions _).when(*).returns(Some(sampleTransactions))

    roundProbe = TestActorRef(Round.props(roundData, fakeDao), roundManagerActor.ref)
  }

  ignore("Round actor") {
    it("should broadcast transactions to facilitators") {
      initBefore
      roundProbe ! StartTransactionProposal(roundData.roundId)

      val expectedProposal =
        TransactionsProposal(roundData.roundId, FacilitatorId(fakeDao.id), sampleTransactions)

      roundManagerActor.expectMsg(BroadcastTransactionProposal(Set(peerA, peerB), expectedProposal))

      roundProbe.underlyingActor.transactionProposals shouldBe Map(
        expectedProposal.facilitatorId -> expectedProposal
      )
    }
    it(
      "should union transactionsProposals and broadcast CheckpointBlock when all proposals arrived "
    ) {
      initBefore
      val transactionSelfProposal =
        TransactionsProposal(roundData.roundId, FacilitatorId(fakeDao.id), sampleTransactions)

      roundProbe ! transactionSelfProposal
      roundProbe ! transactionSelfProposal.copy(
        facilitatorId = FacilitatorId(peerA.peerMetadata.id)
      )
      roundProbe ! transactionSelfProposal.copy(
        facilitatorId = FacilitatorId(peerB.peerMetadata.id)
      )

      roundProbe.underlyingActor.transactionProposals.size shouldBe 3
      val unionBlockProposal =
        roundManagerActor.expectMsgClass(classOf[BroadcastUnionBlockProposal])
      unionBlockProposal.peers shouldBe Set(peerA, peerB)
      unionBlockProposal.proposal.roundId shouldBe roundData.roundId
      unionBlockProposal.proposal.facilitatorId shouldBe roundData.facilitatorId
      unionBlockProposal.proposal.checkpointBlock.transactions shouldBe sampleTransactions
    }
    it(
      "should union checkpoint blocks and accept majority CB"
    ) {
      initBefore
      val checkpointBlock = CheckpointBlock.createCheckpointBlock(
        sampleTransactions,
        roundData.tipsSOE.map(soe => TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash)),
        roundData.messages
      )(fakeDao.keyPair)

      roundProbe.underlyingActor.checkpointBlockProposals.put(roundData.facilitatorId,
                                                              checkpointBlock)
      roundProbe ! ResolveMajorityCheckpointBlock(roundData.roundId)

      roundManagerActor.expectMsg(StopBlockCreationRound(roundData.roundId))

      val watcher = TestProbe()
      watcher watch roundProbe
      watcher.expectTerminated(roundProbe)
    }
  }

}
