package org.constellation.domain.p2p.client

import io.circe.{Decoder, Encoder}
import org.constellation.domain.healthcheck.HealthCheckConsensus.{FetchPeerHealthStatus, SendConsensusHealthStatus}
import org.constellation.domain.healthcheck.HealthCheckConsensusManagerBase.{FetchProposalError, SendProposalError}
import org.constellation.domain.healthcheck.ping.ReconciliationRound.ClusterState
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse

trait HealthCheckClientAlgebra[F[_]] {

  def sendPeerHealthStatus[A <: SendConsensusHealthStatus[_, _, _]: Encoder](
    healthStatus: A
  ): PeerResponse[F, Either[SendProposalError, Unit]]

  def fetchPeerHealthStatus[A <: SendConsensusHealthStatus[_, _, _]: Decoder](
    fetchPeerHealtStatus: FetchPeerHealthStatus
  ): PeerResponse[F, Either[FetchProposalError, A]]

  def getClusterState(): PeerResponse[F, ClusterState]
}
