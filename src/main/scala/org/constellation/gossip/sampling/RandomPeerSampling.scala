package org.constellation.gossip.sampling

import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.p2p.Cluster
import org.constellation.schema.{Id, NodeState}

import scala.collection.immutable.Seq._
import scala.util.Random

/**
  * Peer sampling strategy based on uniform random sample selection
  */
class RandomPeerSampling[F[_]](selfId: Id, clusterStorage: ClusterStorageAlgebra[F])(implicit F: Concurrent[F]) extends PeerSampling[F] {

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  private val fanout: Int = 1

  def selectPaths: F[List[GossipPath]] =
    getPeers
      .map(randomPath(_, fanout))
      .flatMap(paths => paths.toList.traverse(path => generatePathId.map(GossipPath(path, _))))
      .flatTap(
        paths =>
          paths.zipWithIndex.toList.traverse {
            case (path, index) =>
              logger.debug(s"Path $index: ${path.toIndexedSeq.map(_.short).mkString(" -> ")}")
          }
      )

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
    clusterStorage.getPeers
      .map(_.filter {
        case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState)
      }.keySet)

  protected def generatePathId: F[String] = FUUID.randomFUUID.map(_.toString)
}
