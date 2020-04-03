package org.constellation.domain.p2p

import cats.effect.{Concurrent, ContextShift, Sync, Timer}
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.p2p.PeerHealthCheck.{PeerAvailable, PeerHealthCheckStatus, PeerUnresponsive}
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.primitives.Schema.NodeState
import org.constellation.util.Metrics

import scala.concurrent.duration._

class PeerHealthCheck[F[_]: Concurrent: Timer](cluster: Cluster[F], apiClient: ClientInterpreter[F], metrics: Metrics)(
  implicit C: ContextShift[F]
) {
  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  private def checkPeer(peer: PeerClientMetadata): F[PeerHealthCheckStatus] =
    apiClient.metrics
      .checkHealth()
      .run(peer)
      .map(_ => PeerAvailable.asInstanceOf[PeerHealthCheckStatus])
      .handleErrorWith(_ => PeerUnresponsive.asInstanceOf[PeerHealthCheckStatus].pure[F])

  def check(): F[Unit] =
    for {
      _ <- logger.debug("Checking for dead peers")
      peers <- cluster.getPeerInfo.map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
      statuses <- peers.values.toList.traverse(pd => checkPeer(pd.peerMetadata.toPeerClientMetadata).map(pd -> _))
      unresponsivePeers = statuses.count(_._2 == PeerUnresponsive)
      _ <- if (unresponsivePeers > 0) logger.info(s"Found dead peers: ${unresponsivePeers}") else Sync[F].unit
      _ <- markOffline(statuses)
    } yield ()

  def ensureOffline(peerClientMetadata: PeerClientMetadata, attempts: Int = 3): F[Boolean] =
    checkPeer(peerClientMetadata).flatMap {
      case PeerUnresponsive =>
        if (attempts > 0) Timer[F].sleep(5.seconds) >> ensureOffline(peerClientMetadata, attempts - 1) else true.pure[F]
      case _ => false.pure[F]
    }

  private def markOffline(statuses: List[(PeerData, PeerHealthCheckStatus)]): F[Unit] =
    statuses.filter { case (_, status) => status == PeerUnresponsive }.filterA {
      case (pd, _) => ensureOffline(pd.peerMetadata.toPeerClientMetadata)
    }.flatMap(_.traverse {
        case (pd, _) =>
          for {
            _ <- logger.info(s"Marking dead peer: ${pd.peerMetadata.id.short} (${pd.peerMetadata.host}) as offline")
            _ <- cluster.markOfflinePeer(pd.peerMetadata.id)
            _ <- logger.info(s"Broadcasting dead peer: ${pd.peerMetadata.id.short} (${pd.peerMetadata.host})")
            _ <- cluster.broadcastOfflineNodeState(pd.peerMetadata.id)
            _ <- metrics.updateMetricAsync("deadPeer", pd.peerMetadata.host)
          } yield ()
      })
      .void
}

object PeerHealthCheck {

  def apply[F[_]: Concurrent: Timer](cluster: Cluster[F], apiClient: ClientInterpreter[F], metrics: Metrics)(
    implicit C: ContextShift[F]
  ) = new PeerHealthCheck[F](cluster, apiClient, metrics)

  sealed trait PeerHealthCheckStatus
  object PeerAvailable extends PeerHealthCheckStatus
  object PeerUnresponsive extends PeerHealthCheckStatus
}
