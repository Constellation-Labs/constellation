package org.constellation.trust

import cats.effect.{ContextShift, IO}
import org.constellation.ConstellationExecutionContext
import org.constellation.domain.observation.{CheckpointBlockWithMissingSoe, ObservationEvent, SnapshotMisalignment}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class EigenTrustTest
  extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  var trustManager: TrustManager[IO] = _
  var eigenTrust: EigenTrust[IO] = _

  before {
    trustManager = mockTrustManager
    trustManager.observationScoring(*) shouldReturn -0.1

    eigenTrust = new EigenTrust[IO](trustManager)
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
      val observationEvents: List[ObservationEvent] = List(
        SnapshotMisalignment(),
        CheckpointBlockWithMissingSoe("foo")
      )

      val experience = eigenTrust.getExperience(123, observationEvents)
      experience.agent shouldBe 123
      experience.service shouldBe EigenTrust.service
      experience.time shouldBe EigenTrust.time
      experience.outcome >= 0 && experience.outcome <= 1 shouldBe true
    }

    "should convert TrustEdges to Opinions" in {
      val trustEdges = List(
        TrustEdge(1, 2, 0.5),
        TrustEdge(2, 3, -1.0),
        TrustEdge(3, 4, 1.0)
      )

      val opinions = eigenTrust.getOpinions(trustEdges)

      (opinions zip trustEdges).foreach({
        case (opinion, trustEdge) =>
          opinion.agent1 shouldBe trustEdge.src
          opinion.agent2 shouldBe trustEdge.dst
          opinion.service shouldBe EigenTrust.service
          opinion.time shouldBe EigenTrust.time
      })
    }
  }

  private def mockTrustManager: TrustManager[IO] = mock[TrustManager[IO]]
}
