package org.constellation.util

object Rewards {
  val roundingError = 0.000000000001

  val epochOne = 438000
  val epochTwo = 876000
  val epochThree = 1314000
  val epochFour = 1752000

  val epochOneRewards = 0.68493150684//10k/mo, 328.767123287/day
  val epochTwoRewards = 0.34246575342
  val epochThreeRewards = 0.17123287671
  val epochFourRewards = 0.08561643835

  /*
  Partitioning of address space, light nodes have smaller basis that full. Normalizes rewards based on node size
   */
  val partitonChart = Map[String, Set[String]]()

  /*
  Should come from reputation service
   */
  val transitiveReputationMatrix = Map[String, Map[String, Double]]()
  val neighborhoodReputationMatrix = Map[String, Double]()

  /*
  snapshots - 10yrs @ ~20/hr
   */
  val rewardsPool = 1752000

  def validatorRewards(
                        curShapshot: Int,
                       transitiveReputationMatrix: Map[String, Map[String, Double]],
                       neighborhoodReputationMatrix: Map[String, Double],
                       partitonChart: Map[String, Set[String]]
                      )= {
    val trustEntropyMap = shannonEntropy(transitiveReputationMatrix, neighborhoodReputationMatrix)
    val distro = rewardDistribution(partitonChart, trustEntropyMap)
    distro.mapValues(_ * rewardForEpoch(curShapshot))
  }

  def rewardForEpoch(curShapshot: Int) = curShapshot match {
    case num if num >= 0 && num < epochOne  => epochOneRewards
    case num if num >= 438000 && num < epochTwo => epochTwoRewards
    case num if num >= 876000 && num < epochThree => epochThreeRewards
    case num if num >= 1314000 && num < epochFour => epochFourRewards
    case _ => 0d
  }

  def shannonEntropy(
                      transitiveReputationMatrix: Map[String, Map[String, Double]],
                      neighborhoodReputationMatrix: Map[String, Double]
                    ) = {
    val weightedTransitiveReputation = transitiveReputationMatrix.map { case (key, view) =>
      val neighborView = view.map{ case (neighbor, score) => neighborhoodReputationMatrix(key) * score }.sum
        (key, neighborView)
    }
    weightedTransitiveReputation.mapValues{ trust =>
      if (trust == 0.0 ) 0.0
      else - trust * math.log(trust)/math.log(2) }
  }

  def rewardDistribution(partitonChart: Map[String, Set[String]], trustEntropyMap: Map[String, Double]) = {
    val totalSpace = partitonChart.values.map(_.size).max
    val contributions = partitonChart.mapValues( partiton => partiton.size / totalSpace )
    val weightedEntropy = contributions.map { case (address, partitonSize) =>
      val reward = partitonSize * ( 1 - trustEntropyMap(address)) //normalize wrt total partition space
      (address, reward)
    }
    val totalEntropy = weightedEntropy.values.sum
    weightedEntropy.mapValues(_ / totalEntropy )//scale by entropy magnitude
  }
}
