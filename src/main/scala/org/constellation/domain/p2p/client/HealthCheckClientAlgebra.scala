package org.constellation.domain.p2p.client

import org.constellation.domain.healthcheck.HealthCheckConsensus.{
  FetchPeerHealthStatus,
  NotifyAboutMissedConsensus,
  SendPerceivedHealthStatus
}
import org.constellation.domain.healthcheck.HealthCheckConsensusManager.{FetchProposalError, SendProposalError}
import org.constellation.domain.healthcheck.ReconciliationRound.ClusterState
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse

trait HealthCheckClientAlgebra[F[_]] {
  def sendPeerHealthStatus(healthStatus: SendPerceivedHealthStatus): PeerResponse[F, Either[SendProposalError, Unit]]

  def fetchPeerHealthStatus(
    fetchPeerHealtStatus: FetchPeerHealthStatus
  ): PeerResponse[F, Either[FetchProposalError, SendPerceivedHealthStatus]]

  def notifyAboutMissedConsensus(notification: NotifyAboutMissedConsensus): PeerResponse[F, Unit]

  def getClusterState(): PeerResponse[F, ClusterState]
}
