package org.constellation.domain.p2p

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, Sync, Timer}
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.p2p.PeerHealthCheck.{PeerAvailable, PeerHealthCheckStatus, PeerUnresponsive}
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.primitives.Schema.NodeState
import org.constellation.schema.Id
import org.constellation.util.Metrics

import scala.concurrent.duration._

class PeerHealthCheck[F[_]](cluster: Cluster[F], apiClient: ClientInterpreter[F], metrics: Metrics)(
  implicit F: Concurrent[F],
  C: ContextShift[F],
  T: Timer[F]
) {
  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  val periodicCheckTimeout: FiniteDuration = 5.seconds
  val confirmationCheckTimeout: FiniteDuration = 5.seconds
  val ensureSleepTimeBetweenChecks: FiniteDuration = 3.seconds
  val ensureDefaultAttempts = 3

  def check(): F[Unit] =
    for {
      _ <- logger.debug("Checking for dead peers")
      peers <- cluster.getPeerInfo.map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
      statuses <- peers.values.toList
        .traverse(pd => checkPeer(pd.peerMetadata.toPeerClientMetadata, periodicCheckTimeout).map(pd -> _))
      unresponsiveStatuses = statuses.filter { case (_, status) => status == PeerUnresponsive }.map {
        case (pd, _) => pd
      }
      _ <- if (unresponsiveStatuses.nonEmpty)
        logger.info(s"Found dead peers: ${unresponsiveStatuses.size}. Ensuring offline")
      else Sync[F].unit
      unresponsivePeers <- unresponsiveStatuses.filterA { pd =>
        ensureOffline(pd.peerMetadata.toPeerClientMetadata)
      }
      offlinePeers <- confirmUnresponsivePeers(unresponsivePeers)
      _ <- markOffline(offlinePeers)
    } yield ()

  def verify(id: Id): F[PeerHealthCheckStatus] =
    for {
      _ <- logger.info(s"Verifying node responsiveness: ${id}")
      peer <- cluster.getPeerInfo.map(_.get(id))
      result <- peer
        .map(_.peerMetadata.toPeerClientMetadata)
        .fold(PeerUnresponsive.asInstanceOf[PeerHealthCheckStatus].pure[F])(checkPeer(_, confirmationCheckTimeout))
      _ <- logger.info(s"Node responsiveness: ${id} is ${result}")
    } yield result

  private def timeoutTo[A](fa: F[A], after: FiniteDuration, fallback: F[A]): F[A] = // Consider extracting
    F.race(T.sleep(after), fa).flatMap {
      case Left(_)  => fallback
      case Right(a) => F.pure(a)
    }

  private def checkPeer(peer: PeerClientMetadata, timeout: FiniteDuration): F[PeerHealthCheckStatus] =
    timeoutTo(
      apiClient.metrics
        .checkHealth()
        .run(peer)
        .map(_ => PeerAvailable.asInstanceOf[PeerHealthCheckStatus])
        .handleErrorWith(_ => PeerUnresponsive.asInstanceOf[PeerHealthCheckStatus].pure[F]),
      timeout,
      PeerUnresponsive.asInstanceOf[PeerHealthCheckStatus].pure[F]
    )

  private def ensureOffline(peerClientMetadata: PeerClientMetadata, attempts: Int = ensureDefaultAttempts): F[Boolean] =
    checkPeer(peerClientMetadata, confirmationCheckTimeout).flatMap {
      case PeerUnresponsive =>
        if (attempts > 0)
          Timer[F].sleep(ensureSleepTimeBetweenChecks) >> ensureOffline(peerClientMetadata, attempts - 1)
        else true.pure[F]
      case _ => false.pure[F]
    }

  private def confirmUnresponsivePeers(peers: List[PeerData]): F[List[PeerData]] =
    peers.traverse { pd =>
      for {
        _ <- logger.debug(
          s"Waiting for network confirmation before marking a dead peer as offline: ${pd.peerMetadata.id.short} (${pd.peerMetadata.host})"
        )
        statuses <- cluster.broadcast[PeerHealthCheckStatus](
          apiClient.cluster.checkPeerResponsiveness(pd.peerMetadata.id).run,
          Set(pd.peerMetadata.id)
        )
        atLeastOneAvailable = statuses
          .mapValues(_.getOrElse(PeerUnresponsive))
          .exists { case (_, status) => status == PeerAvailable }
        result = if (!atLeastOneAvailable) Some(pd) else None
      } yield result
    }.map(_.flatten)

  private def markOffline(peers: List[PeerData]): F[Unit] =
    peers.traverse { pd =>
      (for {
        _ <- logger.info(s"Marking dead peer: ${pd.peerMetadata.id.short} (${pd.peerMetadata.host}) as offline")
        _ <- F.start(cluster.broadcastOfflineNodeState(pd.peerMetadata.id))
        _ <- cluster.markOfflinePeer(pd.peerMetadata.id)
        _ <- metrics.updateMetricAsync("deadPeer", pd.peerMetadata.host)
      } yield ())
        .handleErrorWith(_ => logger.error("Cannot mark peer as offline - error / skipping to next peer if available"))
    }.void
}

object PeerHealthCheck {

  def apply[F[_]: Concurrent: Timer](cluster: Cluster[F], apiClient: ClientInterpreter[F], metrics: Metrics)(
    implicit C: ContextShift[F]
  ) = new PeerHealthCheck[F](cluster, apiClient, metrics)

  sealed trait PeerHealthCheckStatus
  object PeerAvailable extends PeerHealthCheckStatus
  object PeerUnresponsive extends PeerHealthCheckStatus
}
