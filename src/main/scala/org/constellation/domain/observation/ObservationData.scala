package org.constellation.domain.observation

import org.constellation.schema.{HashGenerator, Id, Signable}

case class ObservationData(
  id: Id,
  event: ObservationEvent,
  time: Long
)(implicit hashGenerator: HashGenerator)
    extends Signable {

  override def hash: String = hashGenerator.hash(this)
}
