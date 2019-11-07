package org.constellation.domain.observation

import org.constellation.schema.Id
import org.constellation.util.Signable

case class ObservationData(
  id: Id,
  event: ObservationEvent,
  time: Long
) extends Signable
