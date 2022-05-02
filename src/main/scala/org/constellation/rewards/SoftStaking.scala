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
  final val testnetAddress: String = "DAG0qE5tkz6cMUD5M2dkqgfV4TQCzUUdAP5MFM9P"
  final val testnetNodes = 217

  def getAddress: String = address

  def weightBySoftStaking(
    ignore: Set[String]
  )(softStakingNodes: Int)(distribution: Map[String, Double]): Map[String, Double] = {

    val softStakingNodesWithTestnet = softStakingNodes + testnetNodes

    val withoutIgnored = distribution -- ignore
    val ignored = distribution -- withoutIgnored.keySet

    val fullNodes = withoutIgnored.size

    val weightedSum = softNodesRatio * softStakingNodesWithTestnet + fullNodesRatio * fullNodes
    val rewards = withoutIgnored.values.sum

    val perSoftNode = (softNodesRatio / weightedSum) * rewards
    val perFullNode = (fullNodesRatio / weightedSum) * rewards

    val totalSoftStakingReward = perSoftNode * softStakingNodes
    val totalTestnetReward = perSoftNode * testnetNodes

    val weighted = withoutIgnored.mapValues(_ => perFullNode) ++ ignored

    weighted + (address -> totalSoftStakingReward) + (testnetAddress -> totalTestnetReward)
  }
}
