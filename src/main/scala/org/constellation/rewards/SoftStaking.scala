package org.constellation.rewards

trait SoftStaking {
  def getAddress: String

  def weightBySoftStaking(ignore: Set[String])(softStakingNodes: Int)(
    distribution: Map[String, Double]
  ): Map[String, Double]
}

object SoftStaking extends SoftStaking {
  private final val address: String = "DAG77VVVRvdZiYxZ2hCtkHz68h85ApT5b2xzdTkn"
  private final val softNodesRatio = 0.4
  private final val fullNodesRatio = 1 - softNodesRatio

  def getAddress: String = address

  def weightBySoftStaking(
    ignore: Set[String]
  )(softStakingNodes: Int)(distribution: Map[String, Double]): Map[String, Double] = {
    val withoutIgnored = distribution -- ignore
    val ignored = distribution -- withoutIgnored.keySet

    val fullNodes = withoutIgnored.size

    val weightedSum = softNodesRatio * softStakingNodes + fullNodesRatio * fullNodes
    val rewards = withoutIgnored.values.sum

    val perSoftNode = (softNodesRatio / weightedSum) * rewards
    val perFullNode = (fullNodesRatio / weightedSum) * rewards

    val totalSoftStakingReward = perSoftNode * softStakingNodes

    val weighted = withoutIgnored.mapValues(_ => perFullNode) ++ ignored

    weighted + (address -> totalSoftStakingReward)
  }
}
