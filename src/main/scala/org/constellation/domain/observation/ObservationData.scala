package org.constellation.domain.observation

import org.constellation.schema.Id
import org.constellation.util.Signable

case class ObservationData(
  pubKeyHex: String,
  event: ObservationEvent,
  time: Long
) extends Signable
