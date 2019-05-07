package org.constellation.util
import org.constellation.DAO

case class HeightEmpty(peer: APIClient)
    extends Exception(s"Empty height found for node: ${peer.baseURI}")
case class CheckPointValidationFailures(peer: APIClient)
    extends Exception(
      s"Checkpoint validation failures found for node: ${peer.baseURI}"
    )
case class InconsistentSnapshotHash(peer: APIClient, hashes: Set[String])
    extends Exception(s"Node: ${peer.baseURI} last snapshot hash differs: $hashes")

object HealthChecker {

  def checkAllMetrics(apis: Seq[APIClient]): Option[Exception] = {
    var hashes: Set[String] = Set.empty
    val it = apis.iterator
    while (it.hasNext) {
      val a = it.next()
      val metrics = a.metrics

      if (hasEmptyHeight(metrics)) return Some(HeightEmpty(a))
      if (hasCheckpointValidationFailures(metrics)) return Some(CheckPointValidationFailures(a))
      hashes ++= Set(a.metrics.getOrElse(Metrics.lastSnapshotHash, "no_snap"))
      if (hashes.size > 1) return Some(InconsistentSnapshotHash(a, hashes))
    }

    None
  }

  def checkTotalBlocksCreatedIncreased(implicit dao: DAO): Unit = {
    dao.metrics.getCountMetric(Metrics.checkpointAccepted)
  }

  def hasEmptyHeight(metrics: Map[String, String]): Boolean = {
    metrics.get(Metrics.heightEmpty).isDefined
  }

  def hasCheckpointValidationFailures(metrics: Map[String, String]): Boolean = {
    metrics.get(Metrics.checkpointValidationFailure).isDefined
  }

}
