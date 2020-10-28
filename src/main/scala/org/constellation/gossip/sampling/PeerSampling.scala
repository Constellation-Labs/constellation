package org.constellation.gossip.sampling

trait PeerSampling[F[_], P] {
  def selectPaths(fanout: Int): F[Seq[IndexedSeq[P]]]
}
