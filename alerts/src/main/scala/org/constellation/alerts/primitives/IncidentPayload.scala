package org.constellation.alerts.primitives

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import org.constellation.alerts.primitives.Severity.Severity

case class IncidentPayload(
  summary: String,
  timestamp: String,
  source: String,
  severity: Severity,
  custom_details: Alert,
)

object IncidentPayload {
  implicit val encoder: Encoder[IncidentPayload] = deriveEncoder
}