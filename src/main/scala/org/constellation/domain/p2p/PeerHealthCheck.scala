package org.constellation.domain.p2p

import cats.effect.concurrent.Ref
import cats.effect.{Clock, Concurrent, ContextShift, Sync, Timer}
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import io.circe.syntax._
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
  CS: ContextShift[F],
  T: Timer[F],
  C: Clock[F]
) {
  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  val periodicCheckTimeout: FiniteDuration = 10.seconds
  val confirmationCheckTimeout: FiniteDuration = 10.seconds
  val ensureSleepTimeBetweenChecks: FiniteDuration = 10.seconds
  val ensureDefaultAttempts = 3
  val cacheAfterChecks = ensureDefaultAttempts
  val cacheForTime = 300.seconds

  val state: Ref[F, Map[Id, PeerHealthCheckStatus]] = Ref.unsafe(Map.empty)

  val getClock = C.monotonic(MILLISECONDS)

  def check(): F[Unit] =
    for {
      _ <- logger.debug("Checking for dead peers")
      peers <- cluster.getPeerInfo.map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
      statuses <- peers.values.toList
        .traverse(pd => checkPeer(pd.peerMetadata.toPeerClientMetadata, periodicCheckTimeout).map(pd -> _))
      unresponsiveStatuses = statuses.filter { case (_, status) => status.isInstanceOf[PeerUnresponsive] }.map {
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
      clock <- getClock
      result <- peer
        .map(_.peerMetadata.toPeerClientMetadata)
        .fold(PeerUnresponsive(clock, 1).asInstanceOf[PeerHealthCheckStatus].pure[F])(
          checkPeer(_, confirmationCheckTimeout)
        )
      _ <- logger.info(s"Node responsiveness: ${id} is ${result}")
    } yield result

  private def timeoutTo[A](fa: F[A], after: FiniteDuration, fallback: F[A]): F[A] = // Consider extracting
    F.race(T.sleep(after), fa).flatMap {
      case Left(_)  => fallback
      case Right(a) => F.pure(a)
    }

  private def checkPeer(peer: PeerClientMetadata, timeout: FiniteDuration): F[PeerHealthCheckStatus] =
    timeoutTo(
      {
        val check: F[PeerHealthCheckStatus] = apiClient.metrics
          .checkHealth()
          .run(peer)
          .flatMap(_ => getClock)
          .map[PeerHealthCheckStatus](clock => PeerAvailable(clock))
          .flatTap { status =>
            state.modify { m =>
              val updated = m.updated(peer.id, status)
              (updated, ())
            }
          }
          .handleErrorWith(
            _ =>
              getClock.flatMap { clock =>
                state.modify { m =>
                  val status = m.get(peer.id).fold(PeerUnresponsive(clock, 1)) {
                    case PeerAvailable(_)            => PeerUnresponsive(clock, 1)
                    case PeerUnresponsive(_, checks) => PeerUnresponsive(clock, checks + 1)
                  }

                  val updated = m.updated(peer.id, status)
                  (updated, status)
                }
              }
          )

        state.get.flatMap[PeerHealthCheckStatus](_.get(peer.id).fold(check) {
          case PeerAvailable(_) => check
          case PeerUnresponsive(lastCheck, checks) =>
            getClock
              .map(now => FiniteDuration(now - lastCheck, MILLISECONDS))
              .map(lastCheck => checks >= cacheAfterChecks && lastCheck < cacheForTime)
              .ifM(
                logger
                  .debug(s"Peer id=${peer.id} is unresponsive. Returning cached value.")
                  .map(_ => PeerUnresponsive(lastCheck, checks)),
                check
              )
        })
      },
      timeout,
      getClock.map(PeerUnresponsive(_, 1))
    )

  private def ensureOffline(peerClientMetadata: PeerClientMetadata, attempts: Int = ensureDefaultAttempts): F[Boolean] =
    checkPeer(peerClientMetadata, confirmationCheckTimeout).flatMap {
      case PeerUnresponsive(_, _) =>
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
        clock <- getClock
        howManyAvailable = statuses
          .mapValues(_.getOrElse(PeerUnresponsive(clock, 1)))
          .count { case (_, status) => status.isInstanceOf[PeerAvailable] }
        _ <- logger.debug(s"[${pd.peerMetadata.host}] How many available: ${howManyAvailable}")
        result = if (howManyAvailable == 0) Some(pd) else None
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

  sealed trait PeerHealthCheckStatus {
    val lastCheck: Long
  }

  object PeerHealthCheckStatus {

    implicit val encodePeerHealthCheckStatus: Encoder[PeerHealthCheckStatus] = deriveEncoder
    implicit val decodePeerHealthCheckStatus: Decoder[PeerHealthCheckStatus] = deriveDecoder
  }

  case class PeerAvailable(lastCheck: Long) extends PeerHealthCheckStatus

  case class PeerUnresponsive(lastCheck: Long, checks: Int) extends PeerHealthCheckStatus
}
