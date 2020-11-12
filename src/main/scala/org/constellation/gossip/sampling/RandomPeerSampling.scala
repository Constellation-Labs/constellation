package org.constellation.gossip.sampling

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.p2p.Cluster
import org.constellation.schema.v2.{Id, NodeState}

import scala.collection.immutable.Seq._
import scala.util.Random

/**
  * Peer sampling strategy based on uniform random sample selection
  */
class RandomPeerSampling[F[_]: Concurrent](selfId: Id, cluster: Cluster[F]) extends PeerSampling[F, Id] {

  def selectPaths(fanout: Int): F[Seq[IndexedSeq[Id]]] =
    getPeers.map(randomPath(_, fanout))

  private def randomPath(peers: Set[Id], fanout: Int): Seq[IndexedSeq[Id]] = {
    def partition(list: IndexedSeq[Id], count: Int): List[IndexedSeq[Id]] = {
      val size = list.size / count
      if (count > 1)
        list.take(size) :: partition(list.drop(size), count - 1)
      else
        list :: Nil
    }

    partition(Random.shuffle(peers.toIndexedSeq), fanout)
      .map(selfId +: _ :+ selfId)
      .map(IndexedSeq[Id](_: _*))
  }

  private def getPeers: F[Set[Id]] =
    cluster.getPeerInfo
      .map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) }.keySet)

}
