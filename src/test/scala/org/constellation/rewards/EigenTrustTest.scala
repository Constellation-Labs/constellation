package org.constellation.rewards

import atb.common.DefaultRandomGenerator
import atb.interfaces.Experience
import cats.effect.concurrent.Ref

import scala.collection.JavaConverters._
import cats.effect.{ContextShift, IO}
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.keytool.KeyUtils
import org.constellation.schema.{Id, observation}
import org.constellation.trust.{TrustEdge, TrustManager}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import atb.trustmodel.{EigenTrust => EigenTrustJ}
import org.constellation.schema.observation.{CheckpointBlockWithMissingSoe, ObservationData}
import org.constellation.serializer.KryoSerializer
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EigenTrustTest
    extends AnyFreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  val kp1 = KeyUtils.makeKeyPair()
  val kp2 = KeyUtils.makeKeyPair()

  val agent1 = Id(KeyUtils.publicKeyToHex(kp1.getPublic))
  val agent2 = Id(KeyUtils.publicKeyToHex(kp2.getPublic))

  val agents = EigenTrustAgents.empty
    .registerAgent(agent1.address)
    .registerAgent(agent2.address)

  var trustManager: TrustManager[IO] = _
  var eigenTrust: EigenTrust[IO] = _
  var dao: DAO = _
  var etj: EigenTrustJ = _

  final val service = 0
  final val time = 0

  final val weight = 0.5d
  final val satisfactoryThreshold = 0.5d
  final val opinionSampleNum = 10
  final val opinionSampleSD = 0.1

  before {
    eigenTrust = new EigenTrust[IO](Id("self"))
    etj = new EigenTrustJ()
    etj.initialize(
      EigenTrust.weight.asInstanceOf[Object],
      EigenTrust.satisfactoryThreshold.asInstanceOf[Object],
      EigenTrust.opinionSampleNum.asInstanceOf[Object],
      EigenTrust.opinionSampleSD.asInstanceOf[Object]
    )
    etj.setRandomGenerator(new DefaultRandomGenerator(0))
    etj.processExperiences(List().asJava)
  }

  def calculateExperience(agent: Int, observations: Seq[Double]): Experience = {
    val best = 1.0
    val sum = observations.sum
    val neg = best + sum
    val outcome = if (neg < 0.0) 0.0 else neg
    new Experience(agent, service, time, outcome)
  }

  "EigenTrustJ serialization/deserialization" - {

    "serializes and deserializes instance" in {
      val serialized = KryoSerializer.serialize[EigenTrustJ](etj)
      val deserialized = KryoSerializer.deserialize(serialized)
      deserialized.isInstanceOf[EigenTrustJ] shouldBe true
    }

    "produces same trust values after deserialization" in {
      val agents = Map(
        "a" -> 1,
        "b" -> 2,
        "c" -> 3
      )

      val snapshotA = Map(
        "a" -> Seq(-0.1, -0.1, -0.1)
      )

      val experiencesA = snapshotA.toList.map {
        case (hash, observations) => calculateExperience(agents(hash), observations)
      }

      etj.processExperiences(experiencesA.asJava)

      val serialized = KryoSerializer.serialize[EigenTrustJ](etj)
      val deserialized = KryoSerializer.deserializeCast[EigenTrustJ](serialized)

      etj.getTrust(0).equals(deserialized.getTrust(0)) shouldBe true
    }

    "still processes experiences properly after deserialization" in {
      val agents = Map(
        "a" -> 1,
        "b" -> 2,
        "c" -> 3
      )

      val snapshotA = Map(
        "a" -> Seq(-0.1, -0.1, -0.1)
      )

      val snapshotB = Map(
        "a" -> Seq(-0.1),
        "c" -> Seq(-0.1)
      )

      val experiencesA = snapshotA.toList.map {
        case (hash, observations) => calculateExperience(agents(hash), observations)
      }

      val experiencesB = snapshotB.toList.map {
        case (hash, observations) => calculateExperience(agents(hash), observations)
      }

      etj.processExperiences(experiencesA.asJava)

      val serialized = KryoSerializer.serialize[EigenTrustJ](etj)
      val deserialized = KryoSerializer.deserializeCast[EigenTrustJ](serialized)

      etj
        .processExperiences(experiencesB.asJava)
      deserialized
        .processExperiences(experiencesB.asJava)

      etj.getTrust(0).equals(deserialized.getTrust(0)) shouldBe true
    }
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
        observation.ObservationData(agent1, CheckpointBlockWithMissingSoe(agent1.address), 123),
        observation.ObservationData(agent2, CheckpointBlockWithMissingSoe(agent2.address), 123),
        observation.ObservationData(agent2, CheckpointBlockWithMissingSoe(agent2.address), 123),
        observation.ObservationData(agent2, CheckpointBlockWithMissingSoe(agent2.address), 123)
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

      opinions.zip(trustEdges).foreach {
        case (opinion, trustEdge) =>
          opinion.agent1 shouldBe trustEdge.src
          opinion.agent2 shouldBe trustEdge.dst
          opinion.service shouldBe EigenTrust.service
          opinion.time shouldBe EigenTrust.time
      }
    }
  }
}
