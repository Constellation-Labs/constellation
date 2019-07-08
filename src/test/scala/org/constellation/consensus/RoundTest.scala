package org.constellation.consensus

import java.util.concurrent.Executors

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.Round._
import org.constellation.consensus.RoundManager.{
  BroadcastLightTransactionProposal,
  BroadcastSelectedUnionBlock,
  BroadcastUnionBlockProposal
}
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.storage.{CheckpointService, MessageService, TransactionService}
import org.constellation.util.Metrics
import org.constellation.{DAO, Fixtures, PeerMetadata}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FunSuiteLike, Ignore, Matchers}

import scala.concurrent.ExecutionContext

class RoundTest
    extends TestKit(ActorSystem("RoundTest"))
    with FunSuiteLike
    with Matchers
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val dao: DAO = mock[DAO]
  dao.keyPair shouldReturn Fixtures.tempKey

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  val checkpointFormationThreshold = 1
  val facilitatorId1 = FacilitatorId(Schema.Id("a"))
  val peerData1 = mock[PeerData]

  val facilitatorId2 = FacilitatorId(Schema.Id("b"))
  val facilitatorId3 = FacilitatorId(Schema.Id("c"))
  val peerData2 = mock[PeerData]

  val readyFacilitators = Map(facilitatorId1.id -> peerData1, facilitatorId2.id -> peerData2)
  val tx1 = Fixtures.dummyTx(dao)
  val tx2 = Fixtures.dummyTx(dao)
  val tx3 = Fixtures.dummyTx(dao)

  val soe = mock[SignedObservationEdge]
  val tips = PulledTips(TipSoe(Seq(soe), None), readyFacilitators)

  val txService = mock[TransactionService[IO]]
  val msgService = mock[MessageService[IO]]
  val cbService = mock[CheckpointService[IO]]
  val roundId = RoundId("round1")

  val dataResolver = mock[DataResolver]

  val roundData = RoundData(
    roundId,
    Set(peerData1, peerData2),
    Set(),
    facilitatorId1,
    List(),
    TipSoe(Seq(), None),
    Seq()
  )
  before {
    peerData1.peerMetadata shouldReturn mock[PeerMetadata]
    peerData1.peerMetadata.id shouldReturn facilitatorId1.id
    peerData1.notification shouldReturn Seq()
    peerData2.peerMetadata shouldReturn mock[PeerMetadata]
    peerData2.peerMetadata.id shouldReturn facilitatorId2.id
    peerData2.notification shouldReturn Seq()

    dao.readyFacilitatorsAsync shouldReturn IO.pure(readyFacilitators)
    dao.pullTips(readyFacilitators) shouldReturn Some(tips)
    dao.threadSafeMessageMemPool shouldReturn mock[ThreadSafeMessageMemPool]
    dao.threadSafeMessageMemPool.pull(1) shouldReturn None
    dao.readyPeers(NodeType.Light) shouldReturn IO.pure(Map())
    txService.pullForConsensus(checkpointFormationThreshold) shouldReturn IO.pure(
      List(tx1, tx2).map(TransactionCacheData(_))
    )
    txService.contains(*) shouldReturn IO.pure(true)
    txService.lookup(*) shouldReturn IO.pure(None)
    dao.transactionService shouldReturn txService
    dao.readyPeers shouldReturn IO.pure(readyFacilitators)
    dao.peerInfo shouldReturn IO.pure(readyFacilitators)
    dao.id shouldReturn facilitatorId1.id
    dao.miscLogger shouldReturn Logger("MiscLogger")

    val msgService = mock[MessageService[IO]]
    msgService.contains(*) shouldReturn IO.pure(true)
    msgService.lookup(*) shouldReturn IO.pure(None)
    dao.messageService shouldReturn msgService

    val metrics = new Metrics()
    dao.metrics shouldReturn metrics

    dao.checkpointService shouldReturn cbService
    dao.checkpointService.accept(any[CheckpointCache]) shouldAnswer ((a: CheckpointCache) => IO.unit)
  }

  val shortTimeouts = ConfigFactory.parseString(
    """constellation {
      consensus {
          union-proposals-timeout = 2s
          arbitrary-data-proposals-timeout = 2s
          checkpoint-block-resolve-majority-timeout = 2s
          accept-resolved-majority-block-timeout = 2s
          form-checkpoint-blocks-timeout = 40s
       }
      }"""
  )

  def createRoundActor(config: Config, parent: ActorRef): TestActorRef[Round] =
    TestActorRef(Round.props(roundData, Seq.empty, Seq.empty, dao, dataResolver, config), parent)

  test("it should send EmptyProposals when transaction proposals are missing") {
    val parentActor = TestProbe()
    createRoundActor(shortTimeouts, parentActor.ref)
    parentActor.expectMsg(EmptyProposals(roundId, "unionProposals", Seq()))
    parentActor.expectNoMessage()
  }

  test(
    "it should send NotEnoughProposals when txs proposals are received partially and round started by others"
  ) {
    dao.id shouldReturn facilitatorId2.id

    val parentActor = TestProbe()
    val roundParticipant = createRoundActor(shortTimeouts, parentActor.ref)

    roundParticipant ! StartTransactionProposal(roundId)
    parentActor.expectMsgType[BroadcastLightTransactionProposal]
    parentActor.expectMsg(EmptyProposals(roundId, "unionProposals", Seq(tx1.hash, tx2.hash)))
    parentActor.expectNoMessage()
  }

  test(
    "it should send NotEnoughProposals when cbs proposals are received partially and round started by others"
  ) {
    dao.id shouldReturn facilitatorId2.id

    val parentActor = TestProbe()
    val roundParticipant = createRoundActor(shortTimeouts, parentActor.ref)
    val underlyingActor = roundParticipant.underlyingActor

    roundParticipant ! StartTransactionProposal(roundId)
    parentActor.expectMsgType[BroadcastLightTransactionProposal]
    roundParticipant ! LightTransactionsProposal(roundId, facilitatorId3, Seq(tx2.hash), Seq(), Seq())
    underlyingActor.unionTransactionProposalsTikTok.isCancelled shouldBe true
    val unionBlock = parentActor.expectMsgType[BroadcastUnionBlockProposal]

    parentActor.expectMsg(EmptyProposals(roundId, "resolveMajorityCheckpointBlock", Seq(tx1.hash, tx2.hash)))
    parentActor.expectNoMessage()
  }

  test("it shouldn't proceed when txs proposals are not received but all peers have sent unions") {
    dao.id shouldReturn facilitatorId2.id

    val parentActor = TestProbe()
    val roundParticipant = createRoundActor(shortTimeouts, parentActor.ref)
    val underlyingActor = roundParticipant.underlyingActor

    roundParticipant ! StartTransactionProposal(roundId)
    parentActor.expectMsgType[BroadcastLightTransactionProposal]

    underlyingActor.unionTransactionProposalsTikTok.isCancelled shouldBe false

    val cb1 = CheckpointBlock.createCheckpointBlock(Seq(tx1), Seq(), Seq(), Seq())(dao.keyPair)
    val cb2 = CheckpointBlock.createCheckpointBlock(Seq(tx2), Seq(), Seq(), Seq())(dao.keyPair)

    roundParticipant ! UnionBlockProposal(roundId, facilitatorId1, cb1)
    roundParticipant ! UnionBlockProposal(roundId, facilitatorId3, cb2)

    parentActor.expectMsg(EmptyProposals(roundId, "unionProposals", Seq(tx1.hash, tx2.hash)))
    parentActor.expectNoMessage()
  }

  test(
    "it shouldn't proceed when union proposals are not received but all peers have sent selected"
  ) {
    dao.id shouldReturn facilitatorId2.id

    val parentActor = TestProbe()
    val roundParticipant = createRoundActor(shortTimeouts, parentActor.ref)
    val underlyingActor = roundParticipant.underlyingActor

    roundParticipant ! StartTransactionProposal(roundId)
    parentActor.expectMsgType[BroadcastLightTransactionProposal]

    underlyingActor.unionTransactionProposalsTikTok.isCancelled shouldBe false

    roundParticipant ! LightTransactionsProposal(roundId, facilitatorId3, Seq(tx2.hash), Seq(), Seq())

    val cb1 = CheckpointBlock.createCheckpointBlock(Seq(tx1), Seq(), Seq(), Seq())(dao.keyPair)

    parentActor.expectMsgType[BroadcastUnionBlockProposal]

    roundParticipant ! SelectedUnionBlock(roundId, facilitatorId1, cb1)
    roundParticipant ! SelectedUnionBlock(roundId, facilitatorId3, cb1)

    parentActor.expectMsg(EmptyProposals(roundId, "resolveMajorityCheckpointBlock", Seq(tx1.hash, tx2.hash)))
    parentActor.expectNoMessage()
  }

  test("it should make the whole flow when receiving proposal at each stage") {
    val parentActor = TestProbe()
    val roundInitiator = createRoundActor(shortTimeouts, parentActor.ref)
    val underlyingActor = roundInitiator.underlyingActor

    //self normally send by RoundManager
    roundInitiator ! LightTransactionsProposal(roundId, facilitatorId1, Seq(tx1.hash), Seq(), Seq())
    //facilitators
    roundInitiator ! LightTransactionsProposal(roundId, facilitatorId2, Seq(tx2.hash), Seq(), Seq())
    roundInitiator ! LightTransactionsProposal(roundId, facilitatorId3, Seq(tx3.hash), Seq(), Seq())

    val unionBlock = parentActor.expectMsgType[BroadcastUnionBlockProposal]
    underlyingActor.unionTransactionProposalsTikTok.isCancelled shouldBe true
    underlyingActor.resolveMajorityCheckpointBlockTikTok.isCancelled shouldBe false
    unionBlock.roundId shouldBe roundId
    unionBlock.peers shouldBe Set(peerData1, peerData2)

    val cb1 = CheckpointBlock.createCheckpointBlock(Seq(tx1), Seq(), Seq(), Seq())(dao.keyPair)
    val cb2 = CheckpointBlock.createCheckpointBlock(Seq(tx2), Seq(), Seq(), Seq())(dao.keyPair)

    roundInitiator ! UnionBlockProposal(roundId, facilitatorId2, cb1)
    roundInitiator ! UnionBlockProposal(roundId, facilitatorId3, cb2)

    underlyingActor.resolveMajorityCheckpointBlockTikTok.isCancelled shouldBe true
    underlyingActor.acceptMajorityCheckpointBlockTikTok.isCancelled shouldBe false

    val selectedBlock = parentActor.expectMsgType[BroadcastSelectedUnionBlock]
    selectedBlock.roundId shouldBe roundId
    selectedBlock.peers shouldBe Set(peerData1, peerData2)

    roundInitiator ! SelectedUnionBlock(roundId, facilitatorId2, cb1)
    roundInitiator ! SelectedUnionBlock(roundId, facilitatorId3, cb2)

    underlyingActor.acceptMajorityCheckpointBlockTikTok.isCancelled shouldBe true
    underlyingActor.resolveMajorityCheckpointBlockTikTok.isCancelled shouldBe true
    val finalBlock = parentActor.expectMsgType[StopBlockCreationRound]
    finalBlock.roundId shouldBe roundId
    finalBlock.maybeCB.isDefined shouldBe true
    underlyingActor.unionTransactionProposalsTikTok.isCancelled shouldBe true
    underlyingActor.acceptMajorityCheckpointBlockTikTok.isCancelled shouldBe true
    underlyingActor.resolveMajorityCheckpointBlockTikTok.isCancelled shouldBe true
    parentActor.expectNoMessage()
  }

  test("it should throw an exception when received a message from previous round stage") {
    val parentActor = TestProbe()
    val roundInitiator = createRoundActor(shortTimeouts, parentActor.ref)
    val underlyingActor = roundInitiator.underlyingActor

    roundInitiator ! LightTransactionsProposal(roundId, facilitatorId1, Seq(tx1.hash), Seq(), Seq())
    roundInitiator ! LightTransactionsProposal(roundId, facilitatorId2, Seq(tx2.hash), Seq(), Seq())
    roundInitiator ! LightTransactionsProposal(roundId, facilitatorId3, Seq(tx3.hash), Seq(), Seq())

    val cb1 = CheckpointBlock.createCheckpointBlock(Seq(tx1), Seq(), Seq(), Seq())(dao.keyPair)
    val cb2 = CheckpointBlock.createCheckpointBlock(Seq(tx2), Seq(), Seq(), Seq())(dao.keyPair)

    roundInitiator ! UnionBlockProposal(roundId, facilitatorId2, cb1)
    roundInitiator ! UnionBlockProposal(roundId, facilitatorId3, cb2)

    an[RuntimeException] should be thrownBy {
      intercept[RuntimeException] {
        roundInitiator ! LightTransactionsProposal(roundId, facilitatorId1, Seq(tx1.hash), Seq(), Seq())
      }
    }
  }

}
