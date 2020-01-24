package org.constellation.rewards

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO}
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.domain.observation.{CheckpointBlockWithMissingSoe, ObservationData, SnapshotMisalignment}
import org.constellation.schema.Id
import org.constellation.trust.{TrustEdge, TrustManager}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class EigenTrustTest
  extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)


  val agent1 = Id("foo")
  val agent2 = Id("bar")

  val agents = EigenTrustAgents.empty
    .registerAgent(agent1)
    .registerAgent(agent2)

  var trustManager: TrustManager[IO] = _
  var eigenTrust: EigenTrust[IO] = _
  var dao: DAO = _

  before {
    trustManager = mockTrustManager
    trustManager.observationScoring(*) shouldReturn -0.1

    eigenTrust = new EigenTrust[IO](trustManager, Id("self"))
  }

  "Normalization from t∈⟨-1;1⟩ to t∈⟨0;1⟩" - {
    "should normalize -1 to 0" in {
      val trust = -1.0
      val normalizedTrust = eigenTrust.normalizeTrust(trust)
      normalizedTrust shouldBe 0.0
    }

    "should normalize 1 to 1" in {
      val trust = 1.0
      val normalizedTrust = eigenTrust.normalizeTrust(trust)
      normalizedTrust shouldBe 1.0
    }

    "should normalize non-border number" in {
      val trust = 0.7
      val normalizedTrust = eigenTrust.normalizeTrust(trust)
      normalizedTrust shouldBe 0.85
    }
  }

  "TrustManager to EigenTrust mappings" - {
    "should convert ObservationEvent to Experience" in {
      val observations: List[ObservationData] = List(
        ObservationData(agent1, SnapshotMisalignment(), 321),
        ObservationData(agent1, CheckpointBlockWithMissingSoe("foo"), 123),
        ObservationData(agent2, CheckpointBlockWithMissingSoe("bar"), 123),
        ObservationData(agent2, CheckpointBlockWithMissingSoe("bar"), 123),
        ObservationData(agent2, CheckpointBlockWithMissingSoe("bar"), 123),
      )

      val experiences = eigenTrust.convertToExperiences(observations, agents)

      experiences.size shouldBe 2
    }

    "should convert TrustEdges to Opinions" in {
      val trustEdges = List(
        TrustEdge(1, 2, 0.5),
        TrustEdge(2, 3, -1.0),
        TrustEdge(3, 4, 1.0)
      )

      val opinions = eigenTrust.convertToOpinions(trustEdges)

      (opinions zip trustEdges).foreach {
        case (opinion, trustEdge) =>
          opinion.agent1 shouldBe trustEdge.src
          opinion.agent2 shouldBe trustEdge.dst
          opinion.service shouldBe EigenTrust.service
          opinion.time shouldBe EigenTrust.time
      }
    }
  }

  private def mockTrustManager: TrustManager[IO] = mock[TrustManager[IO]]
}
