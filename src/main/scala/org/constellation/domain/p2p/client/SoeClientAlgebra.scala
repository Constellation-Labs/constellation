package org.constellation.domain.p2p.client

import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.primitives.Schema.SignedObservationEdge

trait SoeClientAlgebra[F[_]] {
  def getSoe(hash: String): PeerResponse[F, Option[SignedObservationEdge]]
}
