package org.constellation.util

object Rewards {
  val roundingError = 0.000000000001

  // Snapshot epochs. 
  val epochOne = 438000 // Six months have roughly 4380 hours
  val epochTwo = 876000 // epochOne * 2
  val epochThree = 1314000 // epochThree * 2
  val epochFour = 1752000 // epochFour * 2

  /*
  10k per month or about 328.767123287 per day or about 13.698630136958334 per hour, at ~20 snapshots per hour
   */
  val epochOneRewards = 0.68493150684 // 13.698630136958334 / 20 or 50 / 73
  val epochTwoRewards = 0.34246575342 // epochOneRewards / 2
  val epochThreeRewards = 0.17123287671 // epochTwoRewards / 2
  val epochFourRewards = 0.08561643835 // epochThreeRewards / 2

  /*
  Partitioning of address space, light nodes have smaller basis that full. Normalizes rewards based on node size.
   */
  val partitonChart = Map[String, Set[String]]()

  /*
  Should come from reputation service.
   */
  val transitiveReputationMatrix = Map[String, Map[String, Double]]()
  val neighborhoodReputationMatrix = Map[String, Double]()

  /*
  Snapshots: ~20 snapshots per hour for 10 years.
   */
  val rewardsPool = 1752000 // equals epochFour

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
      if (trust == 0.0 ) 0.0
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
    weightedEntropy.mapValues(_ / totalEntropy )// Scale by entropy magnitude
  }
}
