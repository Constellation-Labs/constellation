package org.constellation.util
import cats.implicits._

class MetricFailure(message: String) extends Exception(message)
case class HeightEmpty(nodeId: String) extends MetricFailure(s"Empty height found for node: $nodeId")
case class CheckPointValidationFailures(nodeId: String)
    extends MetricFailure(
      s"Checkpoint validation failures found for node: $nodeId"
    )
case class InconsistentSnapshotHash(nodeId: String, hashes: Set[String])
      extends MetricFailure(s"Node: $nodeId last snapshot hash differs: $hashes")

object HealthChecker {

  def checkAllMetrics(apis: Seq[APIClient]): Either[MetricFailure, Unit] = {
    var hashes: Set[String] = Set.empty
    val it = apis.iterator
    var lastCheck: Either[MetricFailure,Unit] = Right(())
    while (it.hasNext && lastCheck.isRight) {
      val a = it.next()
      val metrics = a.metrics
      lastCheck = checkLocalMetrics(metrics, a.baseURI)
      .orElse {
        hashes ++= Set(metrics.getOrElse(Metrics.lastSnapshotHash, "no_snap"))
        Either.cond(hashes.size == 1, (), InconsistentSnapshotHash(a.baseURI, hashes))
      }
    }
    lastCheck
  }

  def checkLocalMetrics(metrics: Map[String, String],
                        nodeId: String): Either[MetricFailure, Unit] = {
    hasEmptyHeight(metrics, nodeId)
      .orElse(hasCheckpointValidationFailures(metrics, nodeId))
  }

  def hasEmptyHeight(metrics: Map[String, String], nodeId: String): Either[MetricFailure, Unit] = {
    Either.cond(!metrics.contains(Metrics.heightEmpty), (), HeightEmpty(nodeId))
  }

  def hasCheckpointValidationFailures(metrics: Map[String, String],
                                      nodeId: String): Either[MetricFailure, Unit] = {
    Either.cond(!metrics.contains(Metrics.checkpointValidationFailure), (), CheckPointValidationFailures(nodeId))
  }

}
