package org.constellation.util

object Rewards {
  val roundingError = 0.000000000001

  /*
  Rewards computed assuming ~20 snapshots per hour, i.e. ~14,600 snapshots per month.
  Snapshots for a first epoch of 2.5 years. 
   */
  val epochOne = 438000 // = 14,600 * 12 * 2.5 (2.5 years)
  val epochTwo = 876000 // = epochOne * 2 (5 years)
  val epochThree = 1314000 // = epochOne * 3 (7.5 years)
  val epochFour = 1752000 // = epochOne * 4 (10 years)
  val rewardsPool = epochFour

  /*
  10,000 units per month.
   */
  val epochOneRewards = 0.68493150684 // = 10,000 / 14,600
  val epochTwoRewards = 0.34246575342 // = epochOneRewards / 2
  val epochThreeRewards = 0.17123287671 // = epochTwoRewards / 2
  val epochFourRewards = 0.08561643835 // = epochThreeRewards / 2

  /*
  Partitioning of address space, light nodes have smaller basis that full. 
  Normalizes rewards based on node size.
   */
  val partitonChart = Map[String, Set[String]]()

  /*
  Should come from reputation service.
   */
  val transitiveReputationMatrix = Map[String, Map[String, Double]]()
  val neighborhoodReputationMatrix = Map[String, Double]()

  def validatorRewards(
                        curShapshot: Int,
                       transitiveReputationMatrix: Map[String, Map[String, Double]],
                       neighborhoodReputationMatrix: Map[String, Double],
                       partitonChart: Map[String, Set[String]]
                      )= {
    val trustEntropyMap = shannonEntropy(transitiveReputationMatrix, neighborhoodReputationMatrix)
    val distro = rewardDistribution(partitonChart, trustEntropyMap)
    distro.mapValues(_ * rewardDuringEpoch(curShapshot))
  }

  def rewardDuringEpoch(curShapshot: Int) = curShapshot match {
    case num if num >= 0 && num < epochOne => epochOneRewards
    case num if num >= epochOne && num < epochTwo => epochTwoRewards
    case num if num >= epochTwo && num < epochThree => epochThreeRewards
    case num if num >= epochThree && num < epochFour => epochFourRewards
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
      if (trust == 0.0) 0.0
      else - trust * math.log(trust)/math.log(2) }
  }

  def rewardDistribution(partitonChart: Map[String, Set[String]], trustEntropyMap: Map[String, Double]) = {
    val totalSpace = partitonChart.values.map(_.size).max
    val contributions = partitonChart.mapValues( partiton => partiton.size / totalSpace )
    val weightedEntropy = contributions.map { case (address, partitonSize) =>
      val reward = partitonSize * (1 - trustEntropyMap(address)) // Normalize wrt total partition space
      (address, reward)
    }
    val totalEntropy = weightedEntropy.values.sum
    weightedEntropy.mapValues(_ / totalEntropy) // Scale by entropy magnitude
  }
}
