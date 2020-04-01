package org.constellation.consensus

import cats.effect.{Blocker, IO}
import com.typesafe.config.ConfigFactory
import org.constellation._
import org.constellation.consensus.ConsensusManager.{ConsensusStartError, generateRoundId}
import org.constellation.domain.observation.Observation
import org.constellation.primitives.Transaction
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Matchers}

import scala.io.Source

class ConsensusManagerTest
    extends FunSpecLike
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
      dao,
      conf,
      Blocker.liftExecutionContext(ConstellationExecutionContext.unbounded),
      IO.contextShift(ConstellationExecutionContext.bounded)
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

}
