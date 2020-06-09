package org.constellation.domain.observation

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import org.constellation.schema.Id
import org.constellation.util.Signable

case class ObservationData(
  id: Id,
  event: ObservationEvent,
  time: Long
) extends Signable

object ObservationData {
  implicit val observationDataEncoder: Encoder[ObservationData] = deriveEncoder
  implicit val observationDataDecoder: Decoder[ObservationData] = deriveDecoder
}
