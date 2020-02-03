package org.constellation.metrics

import cats.effect.IO
import cats.implicits._
import cats.syntax._
import collection.JavaConverters._
import io.prometheus.client.CollectorRegistry
import org.constellation.alerts.AlertClient
import org.constellation.util.PeriodicIO


case class JVMHeapSize(used: Double, committed: Double, max: Double) {
  val usedRegexp = "jvm_memory_used_bytes"
  val committedRegexp = "jvm_memory_committed_bytes"
  val maxRegexp = "jvm_memory_max_bytes"

  def keys: Set[String] = Set(usedRegexp, committedRegexp, maxRegexp)

  def fromRegistry(registry: CollectorRegistry): Option[JVMHeapSize] = {
    val f = registry.filteredMetricFamilySamples(keys.asJava)
//    registry.filteredMetricFamilySamples(keys.map)
    None
//    (usedRegexp, committedRegexp, maxRegexp).parTra
  }
}


class PeriodicAlertTrigger[F[_]](client: AlertClient[F]) extends PeriodicIO("PeriodicAlertTrigger") {
  override def trigger(): IO[Unit] = {
    val registry = CollectorRegistry.defaultRegistry
    IO.unit
  }
}
