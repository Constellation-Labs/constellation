package org.constellation.gossip.snapshot

import cats.Parallel
import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
import org.constellation.domain.redownload.RedownloadService.SnapshotProposalsAtHeight
import org.constellation.gossip.GossipService
import org.constellation.gossip.sampling.PeerSampling
import org.constellation.gossip.state.{GossipMessage, GossipMessagePathTracker}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.Cluster
import org.constellation.schema.Id

class SnapshotProposalGossipService[F[_]: Concurrent: Timer: Parallel](
  selfId: Id,
  peerSampling: PeerSampling[F],
  cluster: Cluster[F],
  apiClient: ClientInterpreter[F]
) extends GossipService[F, SnapshotProposalGossip](
      selfId,
      peerSampling,
      cluster,
      new GossipMessagePathTracker[F, SnapshotProposalGossip]
    ) {

  override protected def spreadFn(
    nextClientMetadata: PeerResponse.PeerClientMetadata,
    message: GossipMessage[SnapshotProposalGossip]
  ): F[Unit] = apiClient.snapshot.postPeerProposal(message)(nextClientMetadata)

  override protected def validationFn(
    peerClientMetadata: PeerClientMetadata,
    message: GossipMessage[SnapshotProposalGossip]
  ): F[Boolean] =
    for {
      proposals: Option[SnapshotProposalsAtHeight] <- apiClient.snapshot.getPeerProposals(peerClientMetadata.id)(
        peerClientMetadata
      )
      expectedHeight = message.data.height
      expectedHash = message.data.hash
      validProposalAtHeight = proposals
        .flatMap(_.get(expectedHeight))
        .exists(_.hash == expectedHash)
    } yield validProposalAtHeight
}

object SnapshotProposalGossipService {

  def apply[F[_]: Concurrent: Timer: Parallel](
    selfId: Id,
    peerSampling: PeerSampling[F],
    cluster: Cluster[F],
    apiClient: ClientInterpreter[F]
  ): SnapshotProposalGossipService[F] = new SnapshotProposalGossipService(selfId, peerSampling, cluster, apiClient)
}
