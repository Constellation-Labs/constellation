package org.constellation.util

import org.constellation.rewards.Rewards.{rewardDistribution, shannonEntropy}

class RewardsBasedRateLimiter {

  case class DummyFeeTx(id: String, ammt: Double, fee: Option[Double])

  val totalThroughput = 1000d
  val offlineTxThroughput = 0.5
  val validatorThroughput: Double = (1 - offlineTxThroughput) * totalThroughput

  val txSort: Ordering[DummyFeeTx] = Ordering.by(e => (remainingAllowance(e), e.fee))

  /*
  Service keeping track of submitted tx
   */
  def remainingAllowance(id: DummyFeeTx): Int = 0

  def validatorRateAllowance(
    transitiveReputationMatrix: Map[String, Map[String, Double]],
    neighborhoodReputationMatrix: Map[String, Double],
    partitonChart: Map[String, Set[String]]
  ) = {
    val trustEntropyMap = shannonEntropy(transitiveReputationMatrix, neighborhoodReputationMatrix)
    val distro = rewardDistribution(partitonChart, trustEntropyMap)
    distro.mapValues(_ * validatorThroughput)
  }
}
