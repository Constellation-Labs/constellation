package org.constellation.schema.v2.observation

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.schema.v2.Id
import org.constellation.schema.v2.signature.Signable

case class ObservationData(
  id: Id,
  event: ObservationEvent,
  time: Long
) extends Signable

object ObservationData {
  implicit val observationDataEncoder: Encoder[ObservationData] = deriveEncoder
  implicit val observationDataDecoder: Decoder[ObservationData] = deriveDecoder
}
