package org.constellation.domain.p2p.client

import org.constellation.consensus.TipData
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.primitives.Schema.Height
import org.constellation.schema.Id

trait TipsClientAlgebra[F[_]] {
  def getTips(): PeerResponse[F, Map[String, TipData]]

  def getHeights(): PeerResponse[F, List[Height]]

  def getMinTipHeight(): PeerResponse[F, (Id, Long)]
}
