package org.constellation.rewards

trait DTM {
  def getAddress: String
  def weightByDTM(distribution: Map[String, Double]): Map[String, Double]
}

object DTM extends DTM {
  private final val address: String = "DAG0Njmo6JZ3FhkLsipJSppepUHPuTXcSifARfvK"
  private final val avgSnapshotsPerMonth = 43110
  private final val monthly: Double = 2500000d

  private final val perSnapshot = monthly / avgSnapshotsPerMonth

  def getAddress: String = address

  def weightByDTM(distribution: Map[String, Double]): Map[String, Double] = {
    val total = distribution.values.sum

    val reduced = total - perSnapshot
    val perNode = if (reduced >= 0) reduced / distribution.size else 0d

    val weighted = distribution.mapValues(_ => perNode)

    weighted + (address -> perSnapshot)
  }
}
