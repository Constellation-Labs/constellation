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
    (1 to 100).map(idx => (idx.toString, r.nextDouble())).toMap
  val transitiveReputationMatrix: Map[String, Map[String, Double]] =
    (1 to 100).map(idx => (idx.toString, neighborhoodReputationMatrix)).toMap
  val partitonChart = (1 to 100).map(idx => (idx.toString, Set(idx.toString))).toMap

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
    assert(totalDistributionSum - epochOneRewards <= roundingError)
  }
}
