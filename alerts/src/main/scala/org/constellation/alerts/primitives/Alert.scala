package org.constellation.alerts.primitives

import io.circe.Encoder
import io.circe.generic.auto._
import io.circe.syntax._
import org.constellation.alerts.primitives.Severity.Severity

sealed trait Alert {
  def title: String
  def severity: Severity
}

case class CheckpointAcceptanceRateAlert() extends Alert {
  override def title: String = s"Checkpoint acceptance rate"
  override def severity: Severity = Severity.Critical
}

case class DataMigrationAlert() extends Alert {
  override def title: String = s"Data migration"
  override def severity: Severity = Severity.Critical
}

case class PeriodicSnapshotAlignmentAlert(
  ownSnapshots: Seq[String],
  snapshotsToDownload: Seq[String],
  snapshotsToDelete: Seq[String],
) extends Alert {
  override def title: String = s"Periodic snapshot alignment"
  override def severity: Severity = Severity.Critical
}

case class JVMHeapSizeAlert(currentSize: Long, maxSize: Long) extends Alert {
  override def title: String = s"JVM Heap size"
  override def severity: Severity = Severity.Warning
}

case class JVMCPUUsageAlert(currentUsage: Long) extends Alert {
  override def title: String = s"JVM CPU usage"
  override def severity: Severity = Severity.Warning
}

case class ExceptionAlert(exception: String) extends Alert {
  override def title: String = s"Exception"
  override def severity: Severity = Severity.Error
}

case class ErrorAlert(error: String) extends Alert {
  override def title: String = s"Error"
  override def severity: Severity = Severity.Error
}

object Alert {
  implicit val encodeEvent: Encoder[Alert] = Encoder.instance {
    case a @ CheckpointAcceptanceRateAlert()  => a.asJson
    case a @ PeriodicSnapshotAlignmentAlert(_, _, _) => a.asJson
    case a @ JVMHeapSizeAlert(_, _)           => a.asJson
    case a @ JVMCPUUsageAlert(_)              => a.asJson
    case a @ DataMigrationAlert()            => a.asJson
    case a @ ExceptionAlert(_)                => a.asJson
    case a @ ErrorAlert(_)                    => a.asJson
    case _                                    => throw new Throwable("Can't encode Alert")
  }
}
