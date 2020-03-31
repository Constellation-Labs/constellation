package org.constellation.domain.p2p.client

import org.constellation.domain.observation.Observation
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse

trait ObservationClientAlgebra[F[_]] {
  def getObservation(hash: String): PeerResponse[F, Option[Observation]]

  def getBatch(hashes: List[String]): PeerResponse[F, List[(String, Option[Observation])]]
}
