package org.constellation.domain.p2p.client

import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.primitives.Schema.MetricsResult

trait MetricsClientAlgebra[F[_]] {
  def checkHealth(): PeerResponse[F, Unit]

  def getMetrics(): PeerResponse[F, MetricsResult]
}
