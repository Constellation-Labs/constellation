package org.constellation.rewards

trait DTM {
  def getAddress: String
  def weightByDTM(distribution: Map[String, Double]): Map[String, Double]
}

object DTM extends DTM {
  private final val address: String = "DAG4j2r35SDa15iKbjmujcmWbscmjjmqpwAjmz6m"
  private final val avgSnapshotsPerMonth = 43110
  private final val monthly: Double = 50000000000000d // Normalized

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
