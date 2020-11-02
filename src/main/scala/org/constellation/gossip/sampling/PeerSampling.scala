package org.constellation.gossip.sampling

trait PeerSampling[F[_]] {
  def selectPaths: F[List[GossipPath]]
}
