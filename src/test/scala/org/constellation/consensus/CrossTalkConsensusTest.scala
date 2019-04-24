package org.constellation.consensus

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.constellation.DAO
import org.constellation.consensus.CrossTalkConsensus.{NotifyFacilitators, ParticipateInBlockCreationRound, StartNewBlockCreationRound}
import org.constellation.consensus.Round.{LightTransactionsProposal, SelectedUnionBlock, UnionBlockProposal}
import org.constellation.consensus.RoundManager.{BroadcastLightTransactionProposal, BroadcastSelectedUnionBlock, BroadcastUnionBlockProposal}
import org.mockito.integrations.scalatest.IdiomaticMockitoFixture
import org.scalatest.{BeforeAndAfter, FunSuiteLike, OneInstancePerTest}
import scala.concurrent.duration._

class CrossTalkConsensusTest
    extends TestKit(ActorSystem("CrossTalkConsensusTest"))
    with FunSuiteLike
    with IdiomaticMockitoFixture
    with BeforeAndAfter
    with OneInstancePerTest {

  implicit val dao: DAO = mock[DAO]

  val nodeRemoteSender: NodeRemoteSender = mock[NodeRemoteSender]
  val remoteSenderProbe = TestProbe()
  val remoteSender = TestActorRef(NodeRemoteSender.props(nodeRemoteSender), remoteSenderProbe.ref)

  val roundManagerProbe = TestProbe()

  val crossTalkProbe = TestProbe()
  val crossTalkConsensus: TestActorRef[CrossTalkConsensus] =
    TestActorRef(Props(new CrossTalkConsensus(remoteSender) {
      override val roundManager: ActorRef = roundManagerProbe.ref
    }), crossTalkProbe.ref)

  after {
    TestKit.shutdownActorSystem(system)
  }

  test("it should pass StartNewBlockCreationRound to the round manager") {
    val cmd = StartNewBlockCreationRound

    crossTalkConsensus ! cmd

    roundManagerProbe.expectMsg(cmd)
  }

  test("it should pass ParticipateInBlockCreationRound to the round manager") {
    val cmd = mock[ParticipateInBlockCreationRound]

    crossTalkConsensus ! cmd

    roundManagerProbe.expectMsg(cmd)
  }

  test("it should pass BroadcastLightTransactionProposal to the remote sender") {
    val cmd = mock[BroadcastLightTransactionProposal]

    crossTalkConsensus ! cmd

    nodeRemoteSender.broadcastLightTransactionProposal(cmd) was called
  }

  test("it should pass LightTransactionsProposal to the round manager") {
    val cmd = mock[LightTransactionsProposal]

    crossTalkConsensus ! cmd

    roundManagerProbe.expectMsg(cmd)
  }

  test("it should pass UnionBlockProposal to the round manager") {
    val cmd = mock[UnionBlockProposal]

    crossTalkConsensus ! cmd

    roundManagerProbe.expectMsg(cmd)
  }

  test("it should pass BroadcastUnionBlockProposal to the remote sender") {
    val cmd = mock[BroadcastUnionBlockProposal]

    crossTalkConsensus ! cmd

    nodeRemoteSender.broadcastBlockUnion(cmd) was called
  }

  test("it should pass NotifyFacilitators to the remote sender") {
    val cmd = mock[NotifyFacilitators]

    crossTalkConsensus ! cmd

    nodeRemoteSender.notifyFacilitators(cmd) was called
  }

  test("it should pass BroadcastSelectedUnionBlock to the remote sender") {
    val cmd = mock[BroadcastSelectedUnionBlock]

    crossTalkConsensus ! cmd

    nodeRemoteSender.broadcastSelectedUnionBlock(cmd) was called
  }

  test("it should pass SelectedUnionBlock to the round manager") {
    val cmd = mock[SelectedUnionBlock]

    crossTalkConsensus ! cmd

    roundManagerProbe.expectMsg(cmd)
  }
}
