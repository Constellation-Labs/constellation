package org.constellation.util
import java.util

import atb.interfaces.{Experience, Opinion}
import com.typesafe.scalalogging.Logger
import org.constellation.trust.{DataGeneration, EigenTrust, TrustNode}
import org.constellation.trust.EigenTrust.{eigenTrust, nodesWithEdges, opinionsInput, trustMap}
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._
import scala.util.Random

/*
Note: when Experience outcomes are the same, eigentrust scores stay the same.
When Opinions stay the same, eigentrust scores change
 */
class RewardsTest extends FlatSpec {
  import org.constellation.util.Rewards._

  val eigenTrustRes = EigenTrust
  val totalNeighbors = eigenTrustRes.trustMap.size
  val logger = Logger("RewardsTest")

  val r = scala.util.Random
  val epochOneRandom = r.nextInt(epochOne)
  val epochTwoRandom = epochOne + r.nextInt(epochOne)
  val epochThreeRandom = epochTwo + r.nextInt(epochOne)
  val epochFourRandom = epochThree + r.nextInt(epochOne)

  /*
    Should come from reputation, partition management services
   */
  val neighborhoodReputationMatrix = eigenTrustRes.trustMap.map { case (k, v) => (k.toString, v.toDouble) }
  val transitiveReputationMatrix: Map[String, Map[String, Double]] =
    (0 until totalNeighbors).map(idx => (idx.toString, neighborhoodReputationMatrix)).toMap
  val partitonChart = (0 until totalNeighbors).map(idx => (idx.toString, Set(idx.toString))).toMap

  val NaNTest = r.nextInt(totalNeighbors).toString
  neighborhoodReputationMatrix.updated(NaNTest, 0.0)//Ensure perfect behavior doesn't throw Nan

  "rewardForEpoch" should "return correct $DAG ammount" in {
    assert(rewardForEpoch(epochOneRandom) === epochOneRewards)
    assert(rewardForEpoch(epochTwoRandom) === epochTwoRewards)
    assert(rewardForEpoch(epochThreeRandom) === epochThreeRewards)
    assert(rewardForEpoch(epochFourRandom) === epochFourRewards)
  }

  "total rewards disbursed" should "equal total per epoch within error bar" in {
    val rewardsDistro = validatorRewards(0,
      transitiveReputationMatrix,
      neighborhoodReputationMatrix,
      partitonChart)
    val totalDistributionSum = rewardsDistro.values.sum
    println(totalDistributionSum - epochOneRewards)
    assert(totalDistributionSum - epochOneRewards <= roundingError)
  }

  "Experience updates to Eigentrust" should "update the trust matrix" in {
    val experiences = new util.ArrayList[Experience]()
    trustMap.foreach { case (id, trust) =>
      experiences.add(new Experience(id, 0, 0, Random.nextDouble()))//Random double gives us %diff
    }
    eigenTrust.processExperiences(experiences)//Note when outcome is the same scores stay the same
    eigenTrust.calculateTrust()
    val trustMap2 = eigenTrust.getTrust(0).asScala.toMap
    assert(trustMap2 != trustMap)
  }

  "Trust updates to Eigentrust" should "update trust ranking" in {
    val nodesWithEdges2 = DataGeneration.generateTestData()
    val opinionsInput2 = new util.ArrayList[Opinion]()
    nodesWithEdges2.foreach { node =>
      node.edges.foreach { edge =>
        val trust = edge.trust / 2 + 0.5 // Revert from -1 to 1 => 0 to 1
        opinionsInput2.add(new Opinion(edge.src, edge.dst, 0, 1, trust, Random.nextDouble() / 10))
      }
    }
    eigenTrust.processOpinions(opinionsInput2)
    eigenTrust.calculateTrust()
    val trustMap2 = eigenTrust.getTrust(0).asScala.toMap
    assert(trustMap2 != trustMap)
  }

  "Poor performance" should "reduce trust" in {
    val experiences = new util.ArrayList[Experience]()
    trustMap.foreach { case (id, trust) =>
      if (id > 20) experiences.add(new Experience(id, 0, 1, 0d))
      else experiences.add(new Experience(id, 0, 1, Random.nextDouble()))
    }
    eigenTrust.processExperiences(experiences)
    eigenTrust.calculateTrust()
    val trustMap2 = eigenTrust.getTrust(0).asScala.toMap
    assert(trustMap2.filterKeys(_ > 20).forall{ case (id, rank) => rank < trustMap(id)})
    }
  }
