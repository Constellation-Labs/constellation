package org.constellation.util
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec

class RewardsTest extends FlatSpec {
  import org.constellation.util.Rewards._

  val logger = Logger("RewardsTest")

  val r = scala.util.Random
  val epochOneRandom = r.nextInt(epochOne)
  val epochTwoRandom = epochOne + r.nextInt(epochOne)
  val epochThreeRandom = epochTwo + r.nextInt(epochOne)
  val epochFourRandom = epochThree + r.nextInt(epochOne)

  /*
    Should come from reputation, partition management services
   */
  val neighborhoodReputationMatrix: Map[String, Double] =
    (0 to 100).map(idx => (idx.toString, 0.0)).toMap
  val transitiveReputationMatrix: Map[String, Map[String, Double]] =
    (0 to 100).map(idx => (idx.toString, neighborhoodReputationMatrix)).toMap
  val partitonChart = (0 to 100).map(idx => (idx.toString, Set(idx.toString))).toMap

  val thing = r.nextInt(100).toString
  neighborhoodReputationMatrix.updated(thing, 0.0)//Ensure perfect behavior doesn't throw Nan

  "rewardDuringEpoch" should "return correct $DAG ammount" in {
    assert(rewardDuringEpoch(epochOneRandom) === epochOneRewards)
    assert(rewardDuringEpoch(epochTwoRandom) === epochTwoRewards)
    assert(rewardDuringEpoch(epochThreeRandom) === epochThreeRewards)
    assert(rewardDuringEpoch(epochFourRandom) === epochFourRewards)
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
