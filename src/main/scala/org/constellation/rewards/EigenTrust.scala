package org.constellation.rewards

import java.security.SecureRandom

import atb.common.DefaultRandomGenerator
import atb.interfaces.{Experience, Opinion}
import atb.trustmodel.{EigenTrust => EigenTrustJ}
import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import org.constellation.domain.observation.{Observation, ObservationData, ObservationEvent}
import org.constellation.schema.Id
import org.constellation.trust.{DataGeneration, TrustEdge, TrustManager, TrustNode}

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.domain.rewards.StoredRewards
import org.constellation.rewards.EigenTrust.{opinionSampleNum, opinionSampleSD, satisfactoryThreshold, weight}

import scala.util.Random

/**
  * See example usage here : https://github.com/djelenc/alpha-testbed/blob/83e669e69463872aa84017051392c885d4183d1d/src/test/java/atb/trustmodel/EigenTrustTMTest.java#L24
  */
object EigenTrust {
  final val trustRoundingError = 0.001
  final val service = 0
  final val time = 0

  final val weight = 0.5d
  final val satisfactoryThreshold = 0.5d
  final val opinionSampleNum = 10
  final val opinionSampleSD = 0.1

// TODO: Remove commented out code.

  val nodesWithEdges: List[TrustNode] = DataGeneration.generateTestData()

  val opinionsInput = new java.util.ArrayList[Opinion]()

  nodesWithEdges.foreach { node: TrustNode =>
    node.edges.foreach { edge =>
      val trust = edge.trust / 2 + 0.5 // Revert from -1 to 1 => 0 to 1
      //      println(trust)
      opinionsInput.add(new Opinion(edge.src, edge.dst, 0, 0, trust, Random.nextDouble() / 10))
    }
  }

  val eigenTrust = new EigenTrustJ()
  eigenTrust
    .initialize(
      weight.asInstanceOf[Object],
      satisfactoryThreshold.asInstanceOf[Object],
      opinionSampleNum.asInstanceOf[Object],
      opinionSampleSD.asInstanceOf[Object]
    )

  eigenTrust.setRandomGenerator(new DefaultRandomGenerator(0))

  eigenTrust.processExperiences(new java.util.ArrayList[Experience]())
  eigenTrust.processOpinions(opinionsInput)

  eigenTrust.calculateTrust()

  val trustMap: Map[Integer, java.lang.Double] = eigenTrust.getTrust(0).asScala.toMap

  //  trustMap.toSeq.sortBy(_._1).foreach { println }

}

class EigenTrust[F[_]: Concurrent](selfId: Id) {
  private final val secureRandom = SecureRandom.getInstanceStrong
  private final val agents: Ref[F, EigenTrustAgents] = Ref.unsafe(EigenTrustAgents.empty())
  private final val eigenTrustJ: Ref[F, EigenTrustJ] = Ref.unsafe(new EigenTrustJ())

  private implicit val logger = Slf4jLogger.getLogger[F]

  def initializeModel(): F[Unit] = eigenTrustJ.modify { _ =>
    val instance = new EigenTrustJ()
    instance.initialize(
      EigenTrust.weight.asInstanceOf[Object],
      EigenTrust.satisfactoryThreshold.asInstanceOf[Object],
      EigenTrust.opinionSampleNum.asInstanceOf[Object],
      EigenTrust.opinionSampleSD.asInstanceOf[Object]
    )
    instance.setRandomGenerator(new DefaultRandomGenerator(0))
    instance.processExperiences(List().asJava)
    instance.calculateTrust()
    (instance, ())
  }

  def setModel(model: EigenTrustJ): F[Unit] = eigenTrustJ.modify { _ =>
    (model, ())
  }

  def getModel(): F[EigenTrustJ] = eigenTrustJ.modify { et =>
    (et, et)
  }

  def registerSelf(): F[Unit] = registerAgent(selfId)

  def getAgents(): F[EigenTrustAgents] = agents.modify(a => (a, a))

  def setAgents(value: EigenTrustAgents): F[Unit] = agents.modify(_ => (value, ()))

  def clearAgents(): F[Unit] = agents.modify(a => (a.clear(), a)).void

  def registerAgent(id: Id): F[Unit] =
    agents
      .modify(a => {
        val updated = a.registerAgent(id.address)
        (updated, updated)
      })
      .flatMap(
        agents =>
          logger.debug(s"[EigenTrust] Registered EigenTrust agent: ${id.address} -> ${agents.getUnsafe(id.address)}")
      )

  def unregisterAgent(id: Id): F[Unit] =
    agents
      .modify(a => {
        val updated = a.unregisterAgent(id.address)
        (updated, updated)
      })
      .flatMap(_ => logger.debug(s"[EigenTrust] Unregistered EigenTrust agent: ${id.address}"))

  def seed(trustEdges: Seq[TrustEdge]): F[Unit] =
    eigenTrustJ.modify { et =>
      val opinions = convertToOpinions(trustEdges)
      et.processOpinions(opinions.asJava)
      et.calculateTrust()
      (et, ())
    }

  def retrain(observations: Seq[Observation]): F[Unit] = {
    val observationData = observations.map(_.signedObservationData.data)
    for {
      _ <- logger.debug(s"[EigenTrust] Retraining")
      agents <- getAgents()
      experiences = convertToExperiences(observationData, agents).asJava
      _ <- eigenTrustJ.modify { et =>
        et.processExperiences(experiences)
        et.calculateTrust()
        (et, ())
      }
    } yield ()
  }

  def getTrustForAddresses: F[Map[String, Double]] =
    for {
      agents <- getAgents()
      trust <- getTrust()
      trustForAddresses = agents.getAllAsAddresses().transform {
        // If node has no Experiences then it's entropy should be ~0
        case (_, int) => trust.getOrElse(int, 0.0)
      }
    } yield trustForAddresses

  /**
    * Calculates experience outcome as o∈⟨0;1⟩.
    * 1.0 stands for the best experience and 0.0 stands for the worst experience.
    * Each observation moves experience closer to 0.0 by subtracting observation's scoring.
    *
    * @param observationEvents
    * @return Normalized Experience outcome
    */
  private[rewards] def calculateExperienceOutcome(observationEvents: Seq[ObservationEvent]): Double = {
    val bestExperience = 1.0
    val negativeExperiences = observationEvents.map(TrustManager.observationScoring).sum
    val outcome = bestExperience + negativeExperiences
    if (outcome > 0) outcome else 0.0
  }

  private[rewards] def convertToExperiences(
    observations: Seq[ObservationData],
    agents: EigenTrustAgents
  ): Seq[Experience] =
    observations
      .groupBy(_.id)
      .mapValues(data => calculateExperienceOutcome(data.map(_.event)))
      .transform {
        case (id, outcome) => new Experience(agents.getUnsafe(id.address), EigenTrust.service, EigenTrust.time, outcome)
      }
      .values
      .toSeq

  private[rewards] def convertToOpinions(trustEdges: Seq[TrustEdge]): Seq[Opinion] =
    trustEdges
      .map(edge => {
        val trust = normalizeTrust(edge.trust)
        new Opinion(edge.src, edge.dst, EigenTrust.service, EigenTrust.service, trust, secureRandom.nextDouble / 10)
      })

  private[rewards] def getTrust(): F[Map[Integer, Double]] =
    eigenTrustJ.modify { et =>
      val trust = et
        .getTrust(EigenTrust.service)
        .asScala
        .toMap
        .mapValues(_.toDouble)
      (et, trust)
    }

  /**
    * Normalizes trust t∈⟨-1;1⟩ to t∈⟨0;1⟩
    *
    * @param trust
    * @return normalized trust
    */
  private[rewards] def normalizeTrust(trust: Double): Double = trust / 2 + 0.5
}
