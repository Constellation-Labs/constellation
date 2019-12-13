package org.constellation.trust

import java.{lang, util}

import atb.common.DefaultRandomGenerator
import atb.interfaces.{Experience, Opinion}
import atb.trustmodel.{EigenTrust => EigenTrustJ}
import cats.effect.Concurrent
import org.constellation.domain.observation.ObservationEvent

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * See example usage here : https://github.com/djelenc/alpha-testbed/blob/83e669e69463872aa84017051392c885d4183d1d/src/test/java/atb/trustmodel/EigenTrustTMTest.java#L24
  */
object EigenTrust {
  final val trustRoundingError = 0.001
  final val service = 0
  final val time = 0

  val nodesWithEdges: List[TrustNode] = DataGeneration.generateTestData()

  val opinionsInput = new util.ArrayList[Opinion]()

  nodesWithEdges.foreach { node: TrustNode =>
    node.edges.foreach { edge =>
      val trust = edge.trust / 2 + 0.5 // Revert from -1 to 1 => 0 to 1
      //      println(trust)
      opinionsInput.add(new Opinion(edge.src, edge.dst, 0, 0, trust, Random.nextDouble() / 10))
    }
  }

  val eigenTrust = new EigenTrustJ()
  eigenTrust
    .initialize(0.5d.asInstanceOf[Object], 0.5d.asInstanceOf[Object], 10.asInstanceOf[Object], 0.1.asInstanceOf[Object])

  eigenTrust.setRandomGenerator(new DefaultRandomGenerator(0))

  eigenTrust.processExperiences(new util.ArrayList[Experience]())
  eigenTrust.processOpinions(opinionsInput)

  eigenTrust.calculateTrust()

  val trustMap: Map[Integer, lang.Double] = eigenTrust.getTrust(0).asScala.toMap

  //  trustMap.toSeq.sortBy(_._1).foreach { println }

}

class EigenTrust[F[_]: Concurrent](
  trustManager: TrustManager[F]
) {
  final val eigenTrust = new EigenTrustJ()

  def getExperience(agent: Int, observationEvents: Seq[ObservationEvent]): Experience = {
    val outcome = calculateExperienceOutcome(observationEvents)
    new Experience(agent, EigenTrust.service, EigenTrust.time, outcome)
  }

  /**
    * Calculates experience outcome as o∈⟨0;1⟩.
    * 1.0 stands for the best experience and 0.0 stands for the worst experience.
    * Each observation moves experience closer to 0.0 by subtracting observation's scoring.
    *
    * @param observationEvents
    * @return Normalized Experience outcome
    */
  def calculateExperienceOutcome(observationEvents: Seq[ObservationEvent]): Double = {
    val bestExperience = 1.0
    val negativeExperiences = observationEvents.map(trustManager.observationScoring).sum
    val outcome = bestExperience + negativeExperiences
    if (outcome > 0) outcome else 0.0
  }

  def getOpinions(trustEdges: Seq[TrustEdge]): Seq[Opinion] =
    trustEdges
      .map(edge => {
        val trust = normalizeTrust(edge.trust)
        new Opinion(edge.src, edge.dst, EigenTrust.service, EigenTrust.service, trust, Random.nextDouble() / 10)
      })

  /**
    * Normalizes trust t∈⟨-1;1⟩ to t∈⟨0;1⟩
    *
    * @param trust
    * @return normalized trust
    */
  def normalizeTrust(trust: Double): Double = trust / 2 + 0.5
}
