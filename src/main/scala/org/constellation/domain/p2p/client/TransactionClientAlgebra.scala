package org.constellation.domain.p2p.client

import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.transaction.TransactionCacheData

trait TransactionClientAlgebra[F[_]] {
  def getTransaction(hash: String): PeerResponse[F, Option[TransactionCacheData]]

  def getBatch(hashes: List[String]): PeerResponse[F, List[(String, Option[TransactionCacheData])]]
}
