package org.constellation.domain.p2p

import cats.effect.{Concurrent, ContextShift, Sync, Timer}
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.p2p.PeerHealthCheck.{PeerAvailable, PeerHealthCheckStatus, PeerUnresponsive}
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.util.APIClient

import scala.concurrent.duration._

class PeerHealthCheck[F[_]: Concurrent: Timer](cluster: Cluster[F])(implicit C: ContextShift[F]) {
  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  private def checkPeer(peer: APIClient): F[PeerHealthCheckStatus] =
    peer
      .getStringF("health", Map.empty, 5.seconds)(C)
      .map { r =>
        r.body match {
          case Right(body) if (body == "OK") => PeerAvailable
          case _                             => PeerUnresponsive
        }
      }
      .handleErrorWith(_ => PeerUnresponsive.asInstanceOf[PeerHealthCheckStatus].pure[F])

  def check(): F[Unit] =
    for {
      _ <- logger.debug("Checking for dead peers")
      peers <- cluster.getPeerInfo
      statuses <- peers.values.toList.traverse(pd => checkPeer(pd.client).map(pd -> _))
      unresponsivePeers = statuses.count(_._2 == PeerUnresponsive)
      _ <- if (unresponsivePeers > 0) logger.info(s"Found dead peers: ${unresponsivePeers}") else Sync[F].unit
      _ <- markOffline(statuses)
    } yield ()

  def ensureOffline(apiClient: APIClient, attempts: Int = 3): F[Boolean] =
    checkPeer(apiClient).flatMap {
      case PeerUnresponsive =>
        if (attempts > 0) Timer[F].sleep(5.seconds) >> ensureOffline(apiClient, attempts - 1) else true.pure[F]
      case _ => false.pure[F]
    }

  private def markOffline(statuses: List[(PeerData, PeerHealthCheckStatus)]): F[Unit] =
    statuses.filter { case (_, status) => status == PeerUnresponsive }.filterA {
      case (pd, _) => ensureOffline(pd.client)
    }.flatMap(_.traverse {
        case (pd, _) =>
          for {
            _ <- logger.info(s"Marking dead peer: ${pd.client.id.short} (${pd.client.hostName}) as offline")
            _ <- cluster.markOfflinePeer(pd.peerMetadata.id)
            _ <- logger.info(s"Broadcasting dead peer: ${pd.client.id.short} (${pd.client.hostName})")
            _ <- cluster.broadcastOfflineNodeState(pd.client.id)
          } yield ()
      })
      .void
}

object PeerHealthCheck {
  def apply[F[_]: Concurrent: Timer](cluster: Cluster[F])(implicit C: ContextShift[F]) = new PeerHealthCheck[F](cluster)

  sealed trait PeerHealthCheckStatus
  object PeerAvailable extends PeerHealthCheckStatus
  object PeerUnresponsive extends PeerHealthCheckStatus
}
