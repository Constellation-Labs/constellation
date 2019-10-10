package org.constellation.domain.p2p

import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.p2p.PeerHealthCheck.{PeerAvailable, PeerHealthCheckStatus, PeerUnresponsive}
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.util.APIClient

import scala.concurrent.duration._

class PeerHealthCheck[F[_]: Concurrent](cluster: Cluster[F])(implicit C: ContextShift[F]) {
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

  private def forgetUnresponsivePeers(statuses: List[(PeerData, PeerHealthCheckStatus)]): F[Unit] =
    statuses.traverse { a =>
      a._2 match {
        case PeerUnresponsive =>
          logger.info(s"Forgetting peer: ${a._1.client.id.short} (${a._1.client.hostName})") >> cluster.removeDeadPeer(
            a._1
          )
        case _ => Sync[F].unit
      }
    }.void

  def check(): F[Unit] =
    for {
      _ <- logger.info("Checking for dead peers")
      peers <- cluster.getPeerInfo
      statuses <- peers.values.toList.traverse(pd => checkPeer(pd.client).map(pd -> _))
      _ <- logger.info(s"Found dead peers: ${statuses.count(_._2 == PeerUnresponsive)}")
      _ <- forgetUnresponsivePeers(statuses)
    } yield ()
}

object PeerHealthCheck {
  def apply[F[_]: Concurrent](cluster: Cluster[F])(implicit C: ContextShift[F]) = new PeerHealthCheck[F](cluster)

  sealed trait PeerHealthCheckStatus
  object PeerAvailable extends PeerHealthCheckStatus
  object PeerUnresponsive extends PeerHealthCheckStatus
}
