package org.constellation.domain.p2p.client

import org.constellation.domain.p2p.PeerHealthCheck.PeerHealthCheckStatus
import org.constellation.domain.trust.TrustData
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.p2p.Cluster.ClusterNode
import org.constellation.p2p.{JoinedHeight, PeerUnregister, SetNodeStatus}
import org.constellation.schema.v2.Id

trait ClusterClientAlgebra[F[_]] {
  def getInfo(): PeerResponse[F, List[ClusterNode]]

  def setNodeStatus(status: SetNodeStatus): PeerResponse[F, Unit]

  def checkPeerResponsiveness(id: Id): PeerResponse[F, PeerHealthCheckStatus]

  def setJoiningHeight(height: JoinedHeight): PeerResponse[F, Unit]

  def deregister(peerUnregister: PeerUnregister): PeerResponse[F, Unit]

  def getTrust(): PeerResponse[F, TrustData]
}
