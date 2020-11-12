package org.constellation.domain.p2p.client

import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.v2.observation.Observation

trait ObservationClientAlgebra[F[_]] {
  def getObservation(hash: String): PeerResponse[F, Option[Observation]]

  def getBatch(hashes: List[String]): PeerResponse[F, List[(String, Option[Observation])]]
}
