package org.constellation.schema.observation

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.schema.Id
import org.constellation.schema.signature.Signable

case class ObservationData(
  id: Id,
  event: ObservationEvent,
  time: Long
) extends Signable

object ObservationData {
  implicit val observationDataEncoder: Encoder[ObservationData] = deriveEncoder
  implicit val observationDataDecoder: Decoder[ObservationData] = deriveDecoder
}
