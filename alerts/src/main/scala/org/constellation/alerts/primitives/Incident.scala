package org.constellation.alerts.primitives
import io.circe.Encoder
import io.circe.generic.semiauto._

case class Incident(
  payload: IncidentPayload,
  links: Seq[Link] = Seq.empty,
  event_action: String = "trigger",
) {
}

object Incident {
  implicit val encoder: Encoder[Incident] = deriveEncoder
}