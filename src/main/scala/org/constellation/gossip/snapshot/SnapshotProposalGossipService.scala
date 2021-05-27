package org.constellation.gossip.snapshot

import java.security.KeyPair
import cats.Parallel
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.syntax.all._
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.domain.redownload.RedownloadService.SnapshotProposalsAtHeight
import org.constellation.gossip.GossipService
import org.constellation.gossip.sampling.PeerSampling
import org.constellation.gossip.state.{GossipMessage, GossipMessagePathTracker}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.Cluster
import org.constellation.schema.Id
import org.constellation.schema.snapshot.SnapshotProposalPayload
import org.constellation.util.Metrics

class SnapshotProposalGossipService[F[_]: Concurrent: Timer: Parallel: ContextShift](
  selfId: Id,
  keyPair: KeyPair,
  peerSampling: PeerSampling[F],
  clusterStorage: ClusterStorageAlgebra[F],
  apiClient: ClientInterpreter[F],
  metrics: Metrics
) extends GossipService[F, SnapshotProposalPayload](
      selfId,
      keyPair,
      peerSampling,
      clusterStorage,
      metrics,
      new GossipMessagePathTracker[F, SnapshotProposalPayload](metrics)
    ) {

  override protected def spreadFn(
    nextClientMetadata: PeerResponse.PeerClientMetadata,
    message: GossipMessage[SnapshotProposalPayload]
  ): F[Unit] = apiClient.snapshot.postPeerProposal(message)(nextClientMetadata)

  override protected def validationFn(
    peerClientMetadata: PeerClientMetadata,
    message: GossipMessage[SnapshotProposalPayload]
  ): F[Boolean] =
    for {
      proposals: Option[SnapshotProposalsAtHeight] <- apiClient.snapshot.getPeerProposals(peerClientMetadata.id)(
        peerClientMetadata
      )
      expectedHeight = message.payload.proposal.value.height
      expectedHash = message.payload.proposal.value.hash
      validProposalAtHeight = proposals
        .flatMap(_.get(expectedHeight))
        .exists(_.value.hash == expectedHash)
    } yield validProposalAtHeight
}

object SnapshotProposalGossipService {

  def apply[F[_]: Concurrent: Timer: Parallel: ContextShift](
    selfId: Id,
    keyPair: KeyPair,
    peerSampling: PeerSampling[F],
    clusterStorage: ClusterStorageAlgebra[F],
    apiClient: ClientInterpreter[F],
    metrics: Metrics
  ): SnapshotProposalGossipService[F] =
    new SnapshotProposalGossipService(selfId, keyPair, peerSampling, clusterStorage, apiClient, metrics)
}
