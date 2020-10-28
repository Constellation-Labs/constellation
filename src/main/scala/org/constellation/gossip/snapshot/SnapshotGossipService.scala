package org.constellation.gossip.snapshot

import cats.effect.Concurrent
import org.constellation.gossip.sampling.PeerSampling
import org.constellation.gossip.{GossipMessage, GossipService}
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.p2p.Cluster
import org.constellation.schema.Id
import org.constellation.schema.snapshot.Snapshot

class SnapshotGossipService[F[_]: Concurrent](
  selfId: Id,
  peerSampling: PeerSampling[F, Id],
  cluster: Cluster[F],
  apiClient: ClientInterpreter[F]
) extends GossipService[F, Snapshot](selfId, peerSampling, cluster) {

  def spreadSnapshot(message: GossipMessage[Snapshot]): F[Unit] =
    spread(message) { (nextClientMetadata, message) =>
      // TODO: Call proper method on nextClient to send message
      Concurrent[F].unit
    }

  def spreadSnapshot(snapshot: Snapshot, fanout: Int): F[Unit] =
    spread(snapshot, fanout) { (nextClientMetadata, message) =>
      // TODO: Call proper method on nextClient to send message
      Concurrent[F].unit
    }
}
