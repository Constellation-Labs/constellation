package org.constellation.domain.p2p.client

import org.constellation.infrastructure.endpoints.BuildInfoEndpoints.BuildInfoJson
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse

trait BuildInfoClientAlgebra[F[_]] {

  def getBuildInfo(): PeerResponse[F, BuildInfoJson]

  def getGitCommit(): PeerResponse[F, String]
}
