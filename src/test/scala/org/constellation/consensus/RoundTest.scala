package org.constellation.consensus

import java.util.concurrent.Executors

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import cats.effect.IO
import org.constellation.consensus.Round._
import org.constellation.consensus.RoundManager.{BroadcastLightTransactionProposal, BroadcastSelectedUnionBlock, BroadcastUnionBlockProposal}
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema.{CheckpointCache, NodeType, SignedObservationEdge}
import org.constellation.primitives._
import org.constellation.storage.{MessageService, TransactionService}
import org.constellation.util.Metrics
import org.constellation.{DAO, Fixtures, PeerMetadata}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FunSuiteLike, Matchers, OneInstancePerTest}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class RoundTest
    extends TestKit(ActorSystem("RoundTest"))
    with FunSuiteLike
    with Matchers
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with BeforeAndAfter
    with OneInstancePerTest {

  implicit val dao: DAO = mock[DAO]
  implicit val materialize: ActorMaterializer = ActorMaterializer()

  val checkpointFormationThreshold = 1
  val daoId = Schema.Id("a")

  val facilitatorId1 = FacilitatorId(Schema.Id("b"))
  val peerData1 = mock[PeerData]
  peerData1.peerMetadata shouldReturn mock[PeerMetadata]
  peerData1.peerMetadata.id shouldReturn facilitatorId1.id
  peerData1.notification shouldReturn Seq()

  val facilitatorId2 = FacilitatorId(Schema.Id("c"))
  val facilitatorId3 = FacilitatorId(Schema.Id("d"))
  val peerData2 = mock[PeerData]
  peerData2.peerMetadata shouldReturn mock[PeerMetadata]
  peerData2.peerMetadata.id shouldReturn facilitatorId2.id
  peerData2.notification shouldReturn Seq()

  val readyFacilitators = Map(facilitatorId1.id -> peerData1, facilitatorId2.id -> peerData2)

  dao.keyPair shouldReturn Fixtures.tempKey

  val tx1 = Fixtures.dummyTx(dao)
  val tx2 = Fixtures.dummyTx(dao)
  val tx3 = Fixtures.dummyTx(dao)

  val soe = mock[SignedObservationEdge]
  val tips = (Seq(soe), readyFacilitators)

  dao.id shouldReturn daoId
  dao.readyFacilitatorsAsync shouldReturn IO.pure(readyFacilitators)
  dao.peerInfoAsync shouldReturn IO.pure(readyFacilitators)
  dao.pullTips(readyFacilitators) shouldReturn Some(tips)
  dao.threadSafeMessageMemPool shouldReturn mock[ThreadSafeMessageMemPool]
  dao.threadSafeMessageMemPool.pull(1) shouldReturn None
  dao.readyPeersAsync(NodeType.Light) shouldReturn IO.pure(Map())

  val peerManagerProbe = TestProbe()
  val ipManager = mock[IPManager]
  val peerManager = TestActorRef(Props(new PeerManager(ipManager)))
  dao.peerManager shouldReturn peerManager

  val txService = mock[TransactionService[String, TransactionCacheData]]
  txService.pullForConsensus(checkpointFormationThreshold) shouldReturn IO.pure(List(tx1, tx2).map(TransactionCacheData(_)))
  txService.exists shouldReturn (_ => IO.pure(true))
  txService.lookup shouldReturn (_ => IO.pure(None))
  dao.transactionService shouldReturn txService
  dao.readyPeersAsync shouldReturn IO.pure(readyFacilitators)

  val msgService = mock[MessageService]
  msgService.contains shouldReturn (_ => IO.pure(true))
  msgService.lookup shouldReturn (_ => IO.pure(None))
  dao.messageService shouldReturn msgService

  dao.edgeExecutionContext shouldReturn ExecutionContext.fromExecutor(
    Executors.newWorkStealingPool(8)
  )
  val metrics = new Metrics()
  dao.metrics shouldReturn metrics

  dao.threadSafeSnapshotService shouldReturn mock[ThreadSafeSnapshotService]
  dao.threadSafeSnapshotService.accept(*) shouldAnswer ((a: CheckpointCache) => IO.unit)

  val roundId = RoundId("round1")

  val roundData = RoundData(
    roundId,
    Set(peerData1, peerData2),
    Set(),
    facilitatorId1,
    Seq(),
    Seq(),
    Seq()
  )

  val cb1 = CheckpointBlock.createCheckpointBlock(Seq(tx1), Seq(), Seq(), Seq())(dao.keyPair)
  val cb2 = CheckpointBlock.createCheckpointBlock(Seq(tx2), Seq(), Seq(), Seq())(dao.keyPair)
  val cb3 = CheckpointBlock.createCheckpointBlock(Seq(tx3), Seq(), Seq(), Seq())(dao.keyPair)

  val dataResolver = mock[DataResolver]
  val roundProbe = TestProbe()
  val round: TestActorRef[Round] = TestActorRef(
    Props(spy(new Round(roundData, Seq.empty, Seq.empty, dao, dataResolver)))
  )

  after {
    TestKit.shutdownActorSystem(system)
  }

  test(
    "it should pass BroadcastLightTransactionProposal to parent when requested for StartTransactionProposal"
  ) {
    round ! mock[StartTransactionProposal]

    within(2 seconds) {
      expectNoMessage
      round.underlyingActor.passToParentActor(any[BroadcastLightTransactionProposal]) was called
    }
  }

  test("it should combine all received light transactions proposals") {
    val cmd1 = LightTransactionsProposal(
      roundId,
      facilitatorId1,
      Seq(tx1.hash),
      Seq(),
      Seq()
    )

    val cmd2 = LightTransactionsProposal(
      roundId,
      facilitatorId2,
      Seq(tx2.hash),
      Seq(),
      Seq(),
    )

    round ! cmd1
    round ! cmd2

    round.underlyingActor.transactionProposals.size shouldBe 2
  }

  test("it should cancel union transaction proposals timer when received all transaction proposals") {
    round ! LightTransactionsProposal(
      roundId,
      facilitatorId1,
      Seq(tx1.hash),
      Seq(),
      Seq()
    )
    round ! LightTransactionsProposal(
      roundId,
      facilitatorId2,
      Seq(tx2.hash),
      Seq(),
      Seq()
    )
    round ! LightTransactionsProposal(
      roundId,
      FacilitatorId(daoId),
      Seq(tx3.hash),
      Seq(),
      Seq()
    )

    round.underlyingActor.cancelUnionTransactionProposalsTikTok() was called
  }

  test("it should send UnionProposals to self when received all transaction proposals") {
    round ! LightTransactionsProposal(
      roundId,
      facilitatorId1,
      Seq(tx1.hash),
      Seq(),
      Seq()
    )
    round ! LightTransactionsProposal(
      roundId,
      facilitatorId2,
      Seq(tx2.hash),
      Seq(),
      Seq()
    )
    round ! LightTransactionsProposal(
      roundId,
      FacilitatorId(daoId),
      Seq(tx3.hash),
      Seq(),
      Seq()
    )

    round.underlyingActor.unionProposals() was called
  }

  test("it should resolve missing transactions on union block proposals step") {
    dao.readyPeersAsync shouldReturn IO.pure(Map())

    dao.transactionService.exists shouldReturn ((hash: String) => {
      if (hash == tx3.hash || hash == tx2.hash)
        IO.pure(true)
      else
        IO.pure(false)
    })

    dataResolver.resolveTransactions(*, *, *)(3 seconds, dao) shouldReturn IO.pure(None)

    round ! LightTransactionsProposal(
      roundId,
      FacilitatorId(daoId),
      Seq(tx3.hash),
      Seq(),
      Seq()
    )
    round ! LightTransactionsProposal(
      roundId,
      facilitatorId1,
      Seq(tx1.hash),
      Seq(),
      Seq()
    )
    round ! LightTransactionsProposal(
      roundId,
      facilitatorId2,
      Seq(tx2.hash),
      Seq(),
      Seq()
    )

    dataResolver.resolveTransactions(*, List(), None)(3 seconds, dao) was called
  }

  test("it should resolve missing messages on union block proposals step") {
    dao.readyPeersAsync shouldReturn IO.pure(Map())

    dao.messageService.contains shouldReturn (_ => IO.pure(false))

    dataResolver.resolveMessages(*, *, *)(3 seconds, dao) shouldReturn IO.pure(None)

    round ! LightTransactionsProposal(
      roundId,
      FacilitatorId(daoId),
      Seq(tx3.hash),
      Seq(),
      Seq()
    )
    round ! LightTransactionsProposal(
      roundId,
      facilitatorId1,
      Seq(tx1.hash),
      Seq(),
      Seq()
    )
    round ! LightTransactionsProposal(
      roundId,
      facilitatorId2,
      Seq(tx2.hash),
      Seq("msg-hash"),
      Seq()
    )

    dataResolver.resolveMessages("msg-hash", List(), None)(3 seconds, dao) was called
  }

  test("it should broadcast union block proposal after union block proposals step") {
    round ! LightTransactionsProposal(
      roundId,
      FacilitatorId(daoId),
      Seq(tx3.hash),
      Seq(),
      Seq()
    )
    round ! LightTransactionsProposal(
      roundId,
      facilitatorId1,
      Seq(tx1.hash),
      Seq(),
      Seq()
    )
    round ! LightTransactionsProposal(
      roundId,
      facilitatorId2,
      Seq(tx2.hash),
      Seq(),
      Seq()
    )

    round.underlyingActor.passToParentActor(any[BroadcastUnionBlockProposal]) was called
  }

  test("it should combine all received union block proposals") {
    val cmd1 = UnionBlockProposal(
      roundId,
      facilitatorId1,
      mock[CheckpointBlock]
    )

    val cmd2 = UnionBlockProposal(
      roundId,
      facilitatorId2,
      mock[CheckpointBlock]
    )

    round ! cmd1
    round ! cmd2

    within(3 seconds) {
      expectNoMessage
      round.underlyingActor.checkpointBlockProposals.size shouldBe 2
    }
  }

  test(
    "it should send ResolveMajorityCheckpointBlock to self when received all union block proposals"
  ) {
    round ! UnionBlockProposal(roundId, FacilitatorId(daoId), cb1)
    round ! UnionBlockProposal(roundId, facilitatorId1, cb2)
    round ! UnionBlockProposal(roundId, facilitatorId2, cb3)

    round.underlyingActor.resolveMajorityCheckpointBlock(false) was called
  }

  test("it should broadcast selected union block") {
    round ! UnionBlockProposal(roundId, FacilitatorId(daoId), cb1)
    round ! UnionBlockProposal(roundId, facilitatorId1, cb2)
    round ! UnionBlockProposal(roundId, facilitatorId2, cb3)

    round.underlyingActor.passToParentActor(any[BroadcastSelectedUnionBlock]) was called
  }

  test("it should accept majority checkpoint block") {
    round ! SelectedUnionBlock(
      roundId,
      facilitatorId1,
      cb1
    )
    round ! SelectedUnionBlock(
      roundId,
      facilitatorId2,
      cb1
    )

    round ! SelectedUnionBlock(
      roundId,
      facilitatorId3,
      cb2
    )
    round.underlyingActor.passToParentActor(StopBlockCreationRound(RoundId("round1"), Some(cb1))) was called
  }

  test("it should finish round with no finished checkpoint block when accept majority checkpoint block failed") {
    dao.threadSafeSnapshotService.accept(*) shouldAnswer (
      (a: CheckpointCache) => IO.raiseError(TipConflictException(cb1, cb1.transactions.toList.map(_.hash)))
    )
    round ! SelectedUnionBlock(
      roundId,
      facilitatorId1,
      cb1
    )
    round ! SelectedUnionBlock(
      roundId,
      facilitatorId2,
      cb1
    )

    round ! SelectedUnionBlock(
      roundId,
      facilitatorId3,
      cb2
    )

    round.underlyingActor.passToParentActor(StopBlockCreationRound(RoundId("round1"), None)) was called
  }

  test("it should finish round with no finished checkpoint block when selectedCheckpointBlocks are empty") {
    round ! mock[AcceptMajorityCheckpointBlock]

    round.underlyingActor.passToParentActor(StopBlockCreationRound(RoundId("round1"), None)) was called
  }

  // TODO: verify accepted block, then write this unit tests
  ignore("it should broadcast signed block to non facilitators") {}

  test("it should combine all received selected union blocks") {
    round ! SelectedUnionBlock(
      roundId,
      facilitatorId1,
      cb1
    )

    round ! SelectedUnionBlock(
      roundId,
      facilitatorId2,
      cb2
    )

    within(3 seconds) {
      expectNoMessage
      round.underlyingActor.selectedCheckpointBlocks.size shouldBe 2
    }
  }

  test(
    "it should send AcceptMajorityCheckpointBlock to self when received all selected union blocks"
  ) {
    round ! SelectedUnionBlock(roundId, FacilitatorId(daoId), cb1)
    round ! SelectedUnionBlock(roundId, facilitatorId1, cb2)
    round ! SelectedUnionBlock(roundId, facilitatorId2, cb3)

    round.underlyingActor.acceptMajorityCheckpointBlock() was called
  }

  test(
    "it should cancel checkpoint block proposals timer when received all checkpoint block proposals"
  ) {
    round ! UnionBlockProposal(roundId, facilitatorId1, cb1)
    round ! UnionBlockProposal(roundId, facilitatorId2, cb2)
    round ! UnionBlockProposal(roundId, FacilitatorId(daoId), cb3)

    round.underlyingActor.cancelResolveMajorityCheckpointBlockTikTok() was called
  }

}
