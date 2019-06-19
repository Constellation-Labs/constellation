package org.constellation.consensus

import java.util.concurrent.Semaphore

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import cats.effect.{ContextShift, IO}
import com.typesafe.config.ConfigFactory
import org.constellation._
import org.constellation.consensus.CrossTalkConsensus.{
  NotifyFacilitators,
  ParticipateInBlockCreationRound,
  StartNewBlockCreationRound
}
import org.constellation.consensus.Round._
import org.constellation.consensus.RoundManager.{
  BroadcastLightTransactionProposal,
  BroadcastSelectedUnionBlock,
  BroadcastUnionBlockProposal
}
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.storage._
import org.constellation.storage.transactions.TransactionStatus
import org.constellation.util.Metrics
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FunSuiteLike, Matchers, OneInstancePerTest}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

class RoundManagerTest
    extends TestKit(ActorSystem("RoundManagerTest"))
    with FunSuiteLike
    with Matchers
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with BeforeAndAfter
    with OneInstancePerTest {

  implicit val dao: DAO = mock[DAO]
  implicit val materialize: ActorMaterializer = ActorMaterializer()

  val roundManagerProbe = TestProbe("roundManagerSupervisor")

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

  val conf = ConfigFactory.parseString(
    """constellation {
      consensus {
          union-proposals-timeout = 8s
          arbitrary-data-proposals-timeout = 8s
          checkpoint-block-resolve-majority-timeout = 8s
          accept-resolved-majority-block-timeout = 8s
          form-checkpoint-blocks-timeout = 40s
       }
      }"""
  )

  val roundManager: TestActorRef[RoundManager] =
    TestActorRef(Props(spy(new RoundManager(conf))), roundManagerProbe.ref)

  val checkpointFormationThreshold = 1
  val daoId = Schema.Id("a")

  val facilitatorId1 = Schema.Id("b")
  val peerData1 = mock[PeerData]
  peerData1.peerMetadata shouldReturn mock[PeerMetadata]
  peerData1.peerMetadata.id shouldReturn facilitatorId1
  peerData1.notification shouldReturn Seq()

  val facilitatorId2 = Schema.Id("c")
  val peerData2 = mock[PeerData]
  peerData2.peerMetadata shouldReturn mock[PeerMetadata]
  peerData2.peerMetadata.id shouldReturn facilitatorId2
  peerData2.notification shouldReturn Seq()

  val readyFacilitators = Map(facilitatorId1 -> peerData1, facilitatorId2 -> peerData2)

  dao.keyPair shouldReturn Fixtures.tempKey

  val tx1 = Fixtures.dummyTx(dao)
  val tx2 = Fixtures.dummyTx(dao)

  val soe = mock[SignedObservationEdge]
  soe.baseHash shouldReturn "abc"
  val tips = (Seq(soe), readyFacilitators)

  dao.id shouldReturn daoId
  dao.edgeExecutionContext shouldReturn system.dispatcher
  dao.minCheckpointFormationThreshold shouldReturn checkpointFormationThreshold

  dao.readyFacilitatorsAsync shouldReturn IO.pure(readyFacilitators)
  dao.peerInfo shouldReturn IO.pure(readyFacilitators)
  dao.pullTips(readyFacilitators) shouldReturn Some(tips)
  dao.threadSafeMessageMemPool shouldReturn mock[ThreadSafeMessageMemPool]
  dao.threadSafeMessageMemPool.pull(1) shouldReturn None
  dao.checkpointService shouldReturn mock[CheckpointService[IO]]
  dao.checkpointService.contains(*) shouldReturn IO.pure(true)

  val metrics = new Metrics()
  dao.metrics shouldReturn metrics

  dao.messageService shouldReturn mock[MessageService[IO]]
  dao.messageService.arbitraryPool shouldReturn mock[StorageService[IO, ChannelMessageMetadata]]
  dao.messageService.arbitraryPool.toMap() shouldReturn IO.pure(Map.empty)

  dao.transactionService shouldReturn mock[TransactionService[IO]]
  dao.transactionService.getArbitrary shouldReturn IO.pure(Map.empty)
  dao.transactionService.pullForConsensusSafe(checkpointFormationThreshold, *) shouldReturn IO(
    List(tx1, tx2).map(TransactionCacheData(_))
  )

  dao.readyPeers(NodeType.Light) shouldReturn IO.pure(Map())

  val peerManagerProbe = TestProbe()
  val ipManager = mock[IPManager]
  val peerManager = TestActorRef(Props(new PeerManager(ipManager)))
  dao.peerManager shouldReturn peerManager

  after {
    TestKit.shutdownActorSystem(system)
  }

  test("it should not start a new block-creation round if another round is in progress") {
    roundManager ! StartNewBlockCreationRound
    roundManager ! StartNewBlockCreationRound

    within(2 seconds) {
      expectNoMessage
      roundManager.underlyingActor.rounds.size shouldBe 1
    }
  }

  test("it should start a new block-creation round if another round is not in progress") {
    roundManager ! StartNewBlockCreationRound

    within(2 seconds) {
      expectNoMessage
      roundManager.underlyingActor.rounds.size shouldBe 1
    }
  }

  test("it should pass NotifyFacilitators to parent if new round has been started") {
    roundManager ! StartNewBlockCreationRound

    within(2 seconds) {
      expectNoMessage
      roundManager.underlyingActor.passToParentActor(any[NotifyFacilitators]).was(called)
    }
  }

  test("it should pass LightTransactionsProposal to round actor if new round has been started") {
    roundManager ! StartNewBlockCreationRound

    within(2 seconds) {
      expectNoMessage
      roundManager.underlyingActor.passToRoundActor(any[LightTransactionsProposal]).was(called)
    }
  }

  test("it should participate in a new round on ParticipateInBlockCreationRound") {
    val roundData = RoundData(
      RoundId("round1"),
      Set(peerData1, peerData2),
      Set(),
      FacilitatorId(facilitatorId1),
      List(),
      Seq(),
      Seq()
    )
    val cmd = mock[ParticipateInBlockCreationRound]
    cmd.roundData shouldReturn roundData

    roundManager ! cmd

    within(2 seconds) {
      expectNoMessage
      roundManager.underlyingActor.rounds.size shouldBe 1
      val round = roundManager.underlyingActor.rounds.head
      round._2.startedByThisNode shouldBe false
    }
  }

  test(
    "it should pass StartTransactionProposal to round manager if has participated in a new round"
  ) {
    val roundData = RoundData(
      RoundId("round1"),
      Set(peerData1, peerData2),
      Set(),
      FacilitatorId(facilitatorId1),
      List(),
      Seq(),
      Seq()
    )
    val cmd = mock[ParticipateInBlockCreationRound]
    cmd.roundData shouldReturn roundData

    roundManager ! cmd

    within(2 seconds) {
      expectNoMessage
      roundManager.underlyingActor.passToRoundActor(any[StartTransactionProposal]).was(called)
    }
  }

  test("it should pass LightTransactionsProposal to round actor") {
    val cmd = mock[LightTransactionsProposal]
    cmd.roundId shouldReturn RoundId("round1")

    roundManager ! cmd

    roundManager.underlyingActor.passToRoundActor(cmd).was(called)
  }

  test("it should send ResolveMajorityCheckpointBlock to round actor when round timeout has passed") {
    val timersRoundManagerProbe = TestProbe()
    val timersRoundManager: TestActorRef[RoundManager] =
      TestActorRef(Props(spy(new RoundManager(conf), true)), timersRoundManagerProbe.ref)

    timersRoundManager ! StartNewBlockCreationRound

    timersRoundManagerProbe.expectMsgType[NotifyFacilitators]
    Thread.sleep(5000)
    timersRoundManager.underlyingActor.passToRoundActor(any[ResolveMajorityCheckpointBlock]).wasCalled(atLeastOnce)
  }

  test("it should cancel round timeout scheduler on StopBlockCreationRound") {
    roundManager ! StartNewBlockCreationRound

    within(2 seconds) {
      expectNoMessage

      val round = roundManager.underlyingActor.rounds.head

      roundManager ! StopBlockCreationRound(round._1, None, Seq.empty)

      round._2.timeoutScheduler.isCancelled shouldBe true
    }
  }

  test("it should remove round data from rounds Map on StopBlockCreationRound") {
    roundManager ! StartNewBlockCreationRound

    within(2 seconds) {
      expectNoMessage

      val round = roundManager.underlyingActor.rounds.head

      roundManager ! StopBlockCreationRound(round._1, None, Seq.empty)

      roundManager.underlyingActor.rounds.size shouldBe 0
    }
  }

  test("it should allow another round to start after StopBlockCreationRound") {
    roundManager ! StartNewBlockCreationRound

    within(2 seconds) {
      expectNoMessage

      val round = roundManager.underlyingActor.rounds.head

      roundManager ! StopBlockCreationRound(round._1, None, Seq.empty)

      roundManager.underlyingActor.ownRoundInProgress = false
    }
  }

  test("it should release active channels on StopBlockCreationRound") {
    roundManager ! StartNewBlockCreationRound

    within(2 seconds) {
      expectNoMessage

      val round = roundManager.underlyingActor.rounds.head

      val message = mock[ChannelMessage]
      message.signedMessageData shouldReturn mock[SignedData[ChannelMessageData]]
      message.signedMessageData.data shouldReturn mock[ChannelMessageData]
      message.signedMessageData.data.channelId shouldReturn "channel-id"
      val cb = mock[CheckpointBlock]
      cb.messages shouldReturn Seq(message)
      cb.notifications shouldReturn Seq()

      val activeChannels = TrieMap[String, Semaphore]()
      val semaphore = spy(new Semaphore(1))
      activeChannels.put("channel-id", semaphore)
      dao.threadSafeMessageMemPool.activeChannels shouldReturn activeChannels

      roundManager ! StopBlockCreationRound(round._1, Some(cb), Seq.empty)

      semaphore.release().was(called)
    }
  }

  test("it should pass BroadcastLightTransactionProposal to parent actor") {
    val cmd = mock[BroadcastLightTransactionProposal]

    roundManager ! cmd

    roundManager.underlyingActor.passToParentActor(cmd).was(called)
  }

  test("it should pass BroadcastUnionBlockProposal to parent actor") {
    val cmd = mock[BroadcastUnionBlockProposal]

    roundManager ! cmd

    roundManager.underlyingActor.passToParentActor(cmd).was(called)
  }

  test("it should pass UnionBlockProposal to round actor") {
    val cmd = mock[UnionBlockProposal]
    cmd.roundId shouldReturn RoundId("round1")

    roundManager ! cmd

    roundManager.underlyingActor.passToRoundActor(cmd).was(called)
  }

  test("it should close round actor when the round has been finished") {
    roundManager ! StartNewBlockCreationRound

    within(2 seconds) {
      expectNoMessage

      val round = roundManager.underlyingActor.rounds.head

      roundManager ! StopBlockCreationRound(round._1, None, Seq.empty)

      roundManager.underlyingActor.closeRoundActor(round._1).was(called)
    }
  }

  test("it should pass BroadcastSelectedUnionBlock to parent actor") {
    val cmd = mock[BroadcastSelectedUnionBlock]

    roundManager ! cmd

    roundManager.underlyingActor.passToParentActor(cmd).was(called)
  }

  test("it should pass SelectedUnionBlock to round actor") {
    val cmd = mock[SelectedUnionBlock]
    cmd.roundId shouldReturn RoundId("round1")

    roundManager ! cmd

    roundManager.underlyingActor.passToRoundActor(cmd).was(called)
  }

  test("it should remove not accepted transactions") {
    implicit val context: ContextShift[IO] =
      IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

    val semaphore = cats.effect.concurrent.Semaphore[IO](1).unsafeRunSync()
    dao.transactionService shouldReturn new TransactionService[IO](dao, semaphore)

    val tx3 = Fixtures.dummyTx(dao)
    dao.transactionService.put(TransactionCacheData(tx3), TransactionStatus.Pending).unsafeRunSync()
    dao.transactionService.pullForConsensus(1).unsafeRunSync()
    dao.transactionService
      .lookup(tx3.hash, TransactionStatus.InConsensus)
      .unsafeRunSync()
      .map(_.transaction) shouldBe Some(tx3)

    val cb = mock[CheckpointBlock]
    cb.transactions shouldReturn Seq(tx1, tx2)
    cb.messages shouldReturn Seq.empty
    cb.notifications shouldReturn Seq.empty

    roundManager ! StopBlockCreationRound(RoundId("round1"), Some(cb), Seq(tx3.hash))
    dao.transactionService
      .lookup(tx3.hash, TransactionStatus.Pending)
      .unsafeRunSync()
      .map(_.transaction) shouldBe Some(tx3)
  }

}
