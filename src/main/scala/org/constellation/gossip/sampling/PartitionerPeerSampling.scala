package org.constellation.gossip.sampling

import cats.effect.{Concurrent, Sync}
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.trust.TrustDataInternal
import org.constellation.p2p.Cluster
import org.constellation.schema.Id
import org.constellation.schema.NodeState.isNotOffline
import org.constellation.trust.TrustManager
import org.constellation.util.Partitioner._

class PartitionerPeerSampling[F[_]: Concurrent](selfId: Id, cluster: Cluster[F], trustManager: TrustManager[F])
    extends PeerSampling[F] {
  type Partition = IndexedSeq[Id]

  private val partitionCache: Ref[F, List[Partition]] = Ref.unsafe(List.empty[Partition])
  private val trustDataInternalCache: Ref[F, List[TrustDataInternal]] = Ref.unsafe(List.empty[TrustDataInternal])

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  override def selectPaths: F[List[GossipPath]] =
    cachedPartitionsCoverAllPeers.ifM(
      logger.debug("Cached partitions cover all the peers. Returning.") >> partitionCache.get
        .flatMap(partitionsToGossipPaths),
      logger
        .debug("Some nodes are missing in cached partitions. Repartitioning.") >> repartitionWithDefaults() >> partitionCache.get
        .flatMap(partitionsToGossipPaths)
    )

  def repartition(selfTdi: TrustDataInternal, tdi: List[TrustDataInternal]): F[Unit] =
    for {
      partitions <- Sync[F].delay {
        calculatePartitions(selfTdi, tdi)
      }
      _ <- trustDataInternalCache.modify(_ => (tdi, ()))
      _ <- partitionCache.modify(_ => (partitions, ()))
    } yield ()

  private def partitionsToGossipPaths(partitions: List[Partition]): F[List[GossipPath]] =
    partitions.traverse(p => generatePathId.map(GossipPath(p, _)))

  private def cachedPartitionsCoverAllPeers: F[Boolean] =
    for {
      peerIds <- cluster.getPeerInfo.map(_.filter {
        case (_, peerData) => isNotOffline(peerData.peerMetadata.nodeState)
      }.keySet)
      cachedTdi <- trustDataInternalCache.get.map(_.map(_.id).toSet)
    } yield peerIds == cachedTdi

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

  private def calculatePartitions(selfTdi: TrustDataInternal, tdi: List[TrustDataInternal]): List[Partition] = {
    val partitioner = HausdorffPartition(tdi)(selfTdi)
    val partitions = partitioner.nerve
      .filterKeys(_ != 0) // To get rid of self Id
      .values
      .map(_.map(_.id))
      .filterNot(_.isEmpty)
      .map(selfId +: _ :+ selfId)
      .map(IndexedSeq[Id](_: _*))

    partitions.toList
  }

  private def generatePathId: F[String] =
    FUUID.randomFUUID.map(_.toString) // TODO: Consider using pair of hash+proposerId+height+timestamp instead
}

object PartitionerPeerSampling {

  def apply[F[_]: Concurrent](
    selfId: Id,
    cluster: Cluster[F],
    trustManager: TrustManager[F]
  ): PartitionerPeerSampling[F] = new PartitionerPeerSampling(selfId, cluster, trustManager)
}
