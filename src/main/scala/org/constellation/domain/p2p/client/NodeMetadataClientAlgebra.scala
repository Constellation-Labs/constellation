package org.constellation.domain.p2p.client

import org.constellation.PeerMetadata
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.address.AddressCacheData
import org.constellation.util.NodeStateInfo

trait NodeMetadataClientAlgebra[F[_]] {
  def getNodeState(): PeerResponse[F, NodeStateInfo]

  def getAddressBalance(address: String): PeerResponse[F, Option[AddressCacheData]]

  def getPeers(): PeerResponse[F, Seq[PeerMetadata]]
}
