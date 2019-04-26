package org.constellation.util
import com.typesafe.scalalogging.Logger
import org.constellation.trust.EigenTrust
import org.scalatest.FlatSpec

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

  val thing = r.nextInt(totalNeighbors).toString
  neighborhoodReputationMatrix.updated(thing, 0.0)//Ensure perfect behavior doesn't throw Nan

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
}
