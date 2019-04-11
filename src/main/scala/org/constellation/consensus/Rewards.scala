package org.constellation.consensus

object Rewards {

  /*
  Partitioning of address space, light nodes have smaller basis that full. Normalizes rewards based on node size
   */
  val partitonChart = Map[String, Set[String]]
  val transitiveReputationMatrix = Map[String, Map[String, Double]]
  val neighborhoodReputationMatrix = Map[String, Double]
  val rewardsPool = 1752000//snapshots - 10yrs @ ~20/hr

  def distribution(curShapshot: Int) = curShapshot match {
    case num if (num >= 0 && num < 438000)  => 0.68493150684//10k/mo, 328.767123287/day
    case num if (num >= 438000 && num < 876000) => 0.34246575342
    case num if (num >= 876000 && num < 1314000) => 0.17123287671
    case num if (num >= 1314000 && num < 1752000) => 0.08561643835
    case _ => 0d
  }

  def shannonEntropy(
                      transitiveReputationMatrix: Map[String, Map[String, Double]],
                      neighborhoodReputationMatrix: Map[String, Double]
                    ) = {
    val weightedTransitiveReputation = transitiveReputationMatrix.map {
      case (key, view) =>
      val neighborView = view.map{ case (neighbor, score) => neighborhoodReputationMatrix(neighbor) * score }.sum
        (key, neighborView)
    }
    weightedTransitiveReputation.mapValues{ trust => - trust * math.log(trust)/math.log(2)}
  }

  def validatorRewards(partitonChart: Map[String, Set[String]], trustEntropyMap: Map[String, Double]) = {
    val totalSpace = partitonChart.values.map(_.size).max
    val contributions = partitonChart.mapValues { case partiton =>
      partiton.size / totalSpace
    }
    val totalContribution = contributions.values.sum
    contributions.map{ case (address, partitonSize) =>
      (partitonSize / totalContribution) * ( 1 - trustEntropyMap(address)) //normalize wrt total partition space, scale by entropy magnitude
    }
  }
}
