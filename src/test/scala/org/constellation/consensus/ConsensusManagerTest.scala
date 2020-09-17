package org.constellation.consensus

import cats.data.NonEmptyList
import cats.effect.{Blocker, IO}
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import org.constellation._
import org.constellation.consensus.Consensus.{ConsensusDataProposal, FacilitatorId, RoundData, RoundId}
import org.constellation.consensus.ConsensusManager.{ConsensusStartError, generateRoundId}
import org.constellation.domain.observation.{CheckpointBlockInvalid, Observation, ObservationEvent}
import org.constellation.p2p.{MajorityHeight, PeerData}
import org.constellation.primitives.Schema.{CheckpointEdge, EdgeHashType, SignedObservationEdge, TypedEdgeHash}
import org.constellation.primitives.Schema.NodeType.Light
import org.constellation.primitives.{
  ChannelMessageMetadata,
  CheckpointBlock,
  Edge,
  PulledTips,
  TipSoe,
  Transaction,
  TransactionCacheData
}
import org.constellation.schema.Id
import org.constellation.storage.ConcurrentStorageService
import org.constellation.util.{Metrics, SignatureBatch}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.{BeforeAndAfterEach}
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class ConsensusManagerTest
    extends AnyFunSpecLike
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {

  val conf = ConfigFactory.parseString(
    """constellation {
        consensus {
            cleanup-interval = 10s
            start-own-interval = 5s
            union-proposals-timeout = 8s
            arbitrary-data-proposals-timeout = 3s
            checkpoint-block-resolve-majority-timeout = 8s
            accept-resolved-majority-block-timeout = 8s
            form-checkpoint-blocks-timeout = 10s
          }
      }"""
  )

  var consensusManager: ConsensusManager[IO] = _
  var metrics: Metrics = mock[Metrics]
  val consensus: Consensus[IO] = mock[Consensus[IO]]

  val dao: DAO = TestHelpers.prepareMockedDAO()

  implicit val concurrent = IO.ioConcurrentEffect(IO.contextShift(ConstellationExecutionContext.bounded))
  implicit val cs = IO.contextShift(ConstellationExecutionContext.unbounded)
  implicit val timer = IO.timer(ConstellationExecutionContext.unbounded)

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    consensusManager = new ConsensusManager[IO](
      dao.transactionService,
      dao.concurrentTipService,
      dao.checkpointService,
      dao.checkpointAcceptanceService,
      dao.soeService,
      dao.messageService,
      dao.observationService,
      dao.consensusRemoteSender,
      dao.cluster,
      dao.apiClient,
      dao.dataResolver,
      dao,
      conf,
      Blocker.liftExecutionContext(ConstellationExecutionContext.unbounded),
      IO.contextShift(ConstellationExecutionContext.bounded),
      metrics
    )
  }
  describe("syncRoundInProgress") {
    it("should create round") {
      val id = consensusManager.syncRoundInProgress().unsafeRunSync()
      consensusManager.ownConsensus.get.unsafeRunSync().get.roundId shouldBe id
    }

    it("syncRoundInProgress should throw error on multiple consensuses creation") {
      assertThrows[ConsensusStartError] {
        consensusManager.syncRoundInProgress().flatMap(_ => consensusManager.syncRoundInProgress()).unsafeRunSync()
      }
    }
  }
  describe("cleanUpLongRunningConsensus") {
    it("should remove obsolete consensuses") {
      val now = System.currentTimeMillis()
      val active = generateRoundId -> new ConsensusInfo[IO](consensus, None, now)
      val obsolete = generateRoundId -> new ConsensusInfo[IO](
        consensus,
        None,
        now - consensusManager.timeout
      )
      consensusManager.consensuses
        .set(Map(active, obsolete))
        .flatMap(_ => consensusManager.ownConsensus.set(Some(OwnConsensus(generateRoundId, Some(obsolete._2)))))
        .unsafeRunSync()

      val someObseravation = mock[Observation]
      someObseravation.hash shouldReturn "someOb"
      val someTransaction = mock[Transaction]
      someTransaction.hash shouldReturn "someTx"
      consensus.getOwnTransactionsToReturn shouldReturnF Seq(someTransaction)
      consensus.getOwnObservationsToReturn shouldReturnF Seq(someObseravation)
      dao.transactionService.returnToPending(*) shouldReturnF List.empty
      dao.observationService.returnToPending(*) shouldReturnF List.empty
      dao.transactionService.clearInConsensus(*) shouldReturnF List.empty
      dao.observationService.clearInConsensus(*) shouldReturnF List.empty

      consensusManager.cleanUpLongRunningConsensus.unsafeRunSync()

      dao.transactionService.clearInConsensus(Seq("someTx")).wasCalled(twice)
      dao.observationService.clearInConsensus(Seq("someOb")).wasCalled(twice)
      consensusManager.consensuses.get.unsafeRunSync() shouldBe Map(active)
      consensusManager.ownConsensus.get.unsafeRunSync() shouldBe None
    }

  }

  describe("startOwnConsensus") {
    it("after starting new round and getting all data proposals, correct checkpoint block proposal should be created") {
      val owner = Id("f1")
      val p1 = Id("p1")
      val p2 = Id("p2")
      val keyPair = Fixtures.kp
      val tx1 = Fixtures.manuallyCreateTransaction("abc1", "def1")
      val obs1 = Observation.create(Id("abc1"), CheckpointBlockInvalid("abc1", "some_reason"))(keyPair)
      val tx2 = Fixtures.manuallyCreateTransaction("abc2", "def2")
      val obs2 = Observation.create(Id("abc2"), CheckpointBlockInvalid("abc2", "some_reason"))(keyPair)
      val tx3 = Fixtures.manuallyCreateTransaction("abc3", "def3")
      val obs3 = Observation.create(Id("abc3"), CheckpointBlockInvalid("abc3", "some_reason"))(keyPair)
      val p1Data = PeerData(Fixtures.addPeerRequest.copy(id = p1), NonEmptyList(MajorityHeight(Some(10)), List.empty))
      val p2Data = PeerData(Fixtures.addPeerRequest.copy(id = p2), NonEmptyList(MajorityHeight(Some(10)), List.empty))
      val peersMap = Map(p1 -> p1Data, p2 -> p2Data)
      val tips = PulledTips(TipSoe(Seq.empty, None), peersMap).some

      metrics.incrementMetricAsync(*) shouldReturnF (())
      dao.id shouldReturn owner
      dao.transactionService.pullForConsensus(*) shouldReturnF List(TransactionCacheData(tx1))
      dao.readyFacilitatorsAsync shouldReturnF peersMap
      dao.concurrentTipService.pull(*)(*) shouldReturnF tips
      dao.threadSafeMessageMemPool.pull(*) shouldReturn None
      dao.observationService.pullForConsensus(*) shouldReturnF List(obs1)
      dao.readyPeers(Light) shouldReturnF Map.empty
      val mockedArbitraryPool = mock[ConcurrentStorageService[IO, ChannelMessageMetadata]]
      mockedArbitraryPool.toMap shouldReturnF Map.empty
      dao.messageService.arbitraryPool shouldReturn mockedArbitraryPool
      dao.consensusRemoteSender.notifyFacilitators(*) shouldReturnF List(true)
      dao.transactionService.lookup(tx1.hash) shouldReturnF TransactionCacheData(tx1).some
      dao.transactionService.lookup(tx2.hash) shouldReturnF TransactionCacheData(tx2).some
      dao.transactionService.lookup(tx3.hash) shouldReturnF TransactionCacheData(tx3).some
      dao.observationService.lookup(obs1.hash) shouldReturnF obs1.some
      dao.observationService.lookup(obs2.hash) shouldReturnF obs2.some
      dao.observationService.lookup(obs3.hash) shouldReturnF obs3.some
      dao.peerInfo shouldReturnF Map.empty
      dao.consensusRemoteSender.broadcastBlockUnion(*) shouldReturnF (())
      val consensusInfo = consensusManager.startOwnConsensus().unsafeRunSync
      val consensus = consensusInfo.consensus

      val p1ConsensusDataProposal =
        ConsensusDataProposal(RoundId("not_checked"), FacilitatorId(p1), List(tx2), Seq.empty, Seq.empty, Seq(obs2))
      val p2ConsensusDataProposal =
        ConsensusDataProposal(RoundId("not_checked"), FacilitatorId(p2), List(tx3), Seq.empty, Seq.empty, Seq(obs3))

      (consensus.addConsensusDataProposal(p1ConsensusDataProposal) >>
        consensus.addConsensusDataProposal(p2ConsensusDataProposal)).unsafeRunSync

      val cb = CheckpointBlock
        .createCheckpointBlock(Seq(tx1, tx2, tx3), Seq.empty, Seq.empty, Seq.empty, Seq(obs1, obs2, obs3))(dao.keyPair)

      // removing signatures from expected and result - because of salting during signing they will never match
      val expected = removeSignaturesFromCb(cb)
      val result = consensus.checkpointBlockProposals.get.map(_.mapValues(removeSignaturesFromCb)).unsafeRunSync

      result shouldBe Map(
        FacilitatorId(owner) -> expected
      )

    }
  }

  private def removeSignaturesFromCb(cb: CheckpointBlock): CheckpointBlock =
    cb.copy(
      checkpoint = CheckpointEdge(
        Edge(
          cb.checkpoint.edge.observationEdge,
          SignedObservationEdge(
            SignatureBatch(cb.checkpoint.edge.signedObservationEdge.signatureBatch.hash, Seq.empty)
          ),
          cb.checkpoint.edge.data
        )
      )
    )
}
