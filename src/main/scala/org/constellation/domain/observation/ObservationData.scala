package org.constellation.domain.observation

import org.constellation.domain.schema.Id
import org.constellation.util.Signable

case class ObservationData(
  id: Id,
  event: ObservationEvent,
  time: Long
) extends Signable
