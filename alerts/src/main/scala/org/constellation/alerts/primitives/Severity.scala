package org.constellation.alerts.primitives

object Severity extends Enumeration {
  type Severity = String
  val Info = "Info"
  val Warning = "Warning"
  val Error = "Error"
  val Critical = "Critical"
}