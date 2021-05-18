package org.constellation.gossip.checkpoint

import cats.syntax.all._
import cats.Parallel
import cats.effect.{Concurrent, ContextShift, Timer}
import org.constellation.gossip.GossipService
import org.constellation.gossip.sampling.PeerSampling
import org.constellation.gossip.state.{GossipMessage, GossipMessagePathTracker}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.Cluster
import org.constellation.schema.Id
import org.constellation.schema.checkpoint.CheckpointBlockPayload
import org.constellation.util.Metrics

import java.security.KeyPair

class CheckpointBlockGossipService[F[_]: Concurrent: Timer: Parallel: ContextShift](
  selfId: Id,
  keyPair: KeyPair,
  peerSampling: PeerSampling[F],
  cluster: Cluster[F],
  apiClient: ClientInterpreter[F],
  metrics: Metrics
) extends GossipService[F, CheckpointBlockPayload](
      selfId,
      keyPair,
      peerSampling,
      cluster,
      metrics,
      new GossipMessagePathTracker[F, CheckpointBlockPayload](metrics)
    ) {

  override protected def spreadFn(
    nextClientMetadata: PeerResponse.PeerClientMetadata,
    message: GossipMessage[CheckpointBlockPayload]
  ): F[Unit] = apiClient.checkpoint.postFinishedCheckpoint(message)(nextClientMetadata)

  override protected def validationFn(
    peerClientMetadata: PeerClientMetadata,
    message: GossipMessage[CheckpointBlockPayload]
  ): F[Boolean] =
    for {
      block <- apiClient.checkpoint.checkFinishedCheckpoint(
        message.payload.block.value.checkpointCacheData.checkpointBlock.soeHash
      )(peerClientMetadata)
      expectedHash = message.payload.block.value.checkpointCacheData.checkpointBlock.soeHash
      validBlock = block.map(_.checkpointBlock.soeHash).contains(expectedHash)
    } yield validBlock
}

object CheckpointBlockGossipService {

  def apply[F[_]: Concurrent: Timer: Parallel: ContextShift](
    selfId: Id,
    keyPair: KeyPair,
    peerSampling: PeerSampling[F],
    cluster: Cluster[F],
    apiClient: ClientInterpreter[F],
    metrics: Metrics
  ): CheckpointBlockGossipService[F] =
    new CheckpointBlockGossipService[F](selfId, keyPair, peerSampling, cluster, apiClient, metrics)
}
