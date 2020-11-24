package org.constellation.gossip.sampling

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.trust.TrustDataInternal
import org.constellation.p2p.Cluster
import org.constellation.schema.Id
import org.constellation.trust.TrustManager
import org.constellation.util.Partitioner._

class PartitionerPeerSampling[F[_]: Concurrent](selfId: Id, cluster: Cluster[F], trustManager: TrustManager[F])
    extends PeerSampling[F] {
  private val partitionCache: Ref[F, List[GossipPath]] = Ref.unsafe(List.empty[GossipPath])
  private val trustDataInternalCache: Ref[F, List[TrustDataInternal]] = Ref.unsafe(List.empty[TrustDataInternal])

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  override def selectPaths: F[List[GossipPath]] =
    cachedPartitionsCoverAllPeers.ifM(
      logger.debug("Cached partitions cover all the peers. Returning.") >> partitionCache.get,
      logger
        .debug("Some nodes are missing in cached partitions. Repartitioning.") >> repartitionWithDefaults() >> partitionCache.get
    )

  def repartition(selfTdi: TrustDataInternal, tdi: List[TrustDataInternal]): F[Unit] =
    for {
      partitions <- calculatePartitions(selfTdi, tdi)
      _ <- trustDataInternalCache.modify(_ => (tdi, ()))
      _ <- partitionCache.modify(_ => (partitions, ()))
    } yield ()

  private def cachedPartitionsCoverAllPeers: F[Boolean] =
    for {
      peerIds <- cluster.getPeerInfo.map(_.keySet) // TODO: Should we filter out offline nodes?
      cachedTdi <- trustDataInternalCache.get.map(_.map(_.id).toSet)
    } yield peerIds.subsetOf(cachedTdi)

  private def repartitionWithDefaults(): F[Unit] =
    for {
      selfTdi <- trustManager.getTrustDataInternalSelf
      peerIds <- cluster.getPeerInfo.map(_.keySet)
      cachedTdi <- trustDataInternalCache.get
      missingTdi = peerIds -- cachedTdi.map(_.id)
      _ <- logger.debug(
        s"Missing TrustDataInternal of following peers: ${missingTdi.map(_.short)}. Repartitioning with default values."
      )
      updatedTdi = cachedTdi ++ missingTdi.map(TrustDataInternal(_, peerIds.map(_ -> 0.0).toMap))
      _ <- repartition(selfTdi, updatedTdi)
    } yield ()

  private def calculatePartitions(selfTdi: TrustDataInternal, tdi: List[TrustDataInternal]): F[List[GossipPath]] = {
    val partitioner = HausdorffPartition(tdi)(selfTdi)
    val partitions = partitioner.nerve
      .filterKeys(_ != 0) // To get rid of self Id
      .values
      .map(_.map(_.id))
      .filterNot(_.isEmpty)
      .map(selfId +: _ :+ selfId)
      .map(IndexedSeq[Id](_: _*))

    partitions.toList.traverse(partition => generatePathId.map(GossipPath(partition, _)))
  }

  private def generatePathId: F[String] =
    FUUID.randomFUUID.map(_.toString) // TODO: Consider using pair of hash+proposerId+height+timestamp instead
}
