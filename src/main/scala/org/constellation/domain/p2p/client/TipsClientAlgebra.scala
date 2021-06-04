package org.constellation.domain.p2p.client

import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.checkpoint.TipData
import org.constellation.schema.{Height, Id}

trait TipsClientAlgebra[F[_]] {
  def getTips(): PeerResponse[F, Set[(String, Height)]]

  def getHeights(): PeerResponse[F, List[Height]]

  def getMinTipHeight(): PeerResponse[F, (Id, Long)]
}
