package org.constellation.domain.p2p

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Sync, Timer}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.domain.p2p.PeerHealthCheck.{PeerAvailable, PeerHealthCheckStatus, PeerUnresponsive}
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.schema.{Id, NodeState}
import org.constellation.util.Metrics

import scala.concurrent.duration._
import scala.util.Random

class PeerHealthCheck[F[_]](
  clusterStorage: ClusterStorageAlgebra[F],
  cluster: Cluster[F],
  apiClient: ClientInterpreter[F],
  metrics: Metrics,
  unboundedHealthBlocker: Blocker,
  healthHttpPort: String
)(
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

  val state: Ref[F, Map[Id, PeerHealthCheckStatus]] = Ref.unsafe(Map.empty)

  val getClock = C.monotonic(MILLISECONDS)

  def check(excludePeers: Set[Id], checkThesePeers: Set[Id]): F[List[PeerData]] =
    for {
      _ <- logger.debug("Checking for dead peers")
      peers <- clusterStorage.getPeers
        .map(_.filter { case (_, pd) => NodeState.canBeCheckedForHealth(pd.peerMetadata.nodeState) })
      peersToCheck <- pickRandomPeers((peers -- excludePeers -- checkThesePeers).values.toList, 3) // avoid duplicate runs for the same id
        .map(
          _ ++
            peers
              .filterKeys(checkThesePeers.contains)
              .values
              .toList // we could pick random peers only if checkThesePeers contains less then 3 nodes
        )
      _ <- logger.debug(s"Checking peers: ${peersToCheck.map(_.peerMetadata.id.short)}")
      statuses <- peersToCheck
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
    } yield unresponsivePeers

  def pickRandomPeers(peers: List[PeerData], count: Int): F[List[PeerData]] = F.delay(
    Random.shuffle(peers).take(count)
  )

  private def timeoutTo[A](fa: F[A], after: FiniteDuration, fallback: F[A]): F[A] = // Consider extracting
    F.race(T.sleep(after), fa).flatMap {
      case Left(_)  => fallback
      case Right(a) => F.pure(a)
    }

  def checkPeer(peer: PeerClientMetadata, timeout: FiniteDuration): F[PeerHealthCheckStatus] =
    timeoutTo(
      {
        PeerResponse
          .run(
            apiClient.metrics
              .checkHealth(),
            unboundedHealthBlocker
          )(peer.copy(port = healthHttpPort))
          .map[PeerHealthCheckStatus](_ => PeerAvailable(peer.id))
          .handleErrorWith(
            e =>
              logger.warn(s"Error checking peer responsiveness. ErrorMessage: ${e.getMessage}") >>
                PeerUnresponsive(peer.id)
                  .asInstanceOf[PeerHealthCheckStatus]
                  .pure[F]
          )
      },
      timeout,
      PeerUnresponsive(peer.id).asInstanceOf[PeerHealthCheckStatus].pure[F]
    )

  private def ensureOffline(peerClientMetadata: PeerClientMetadata, attempts: Int = ensureDefaultAttempts): F[Boolean] =
    logger.debug(
      s"Ensuring node with id=${peerClientMetadata.id.medium} is offline, left attempts including this one is $attempts."
    ) >>
      checkPeer(peerClientMetadata, confirmationCheckTimeout).flatMap {
        case PeerUnresponsive(_) =>
          if (attempts > 0)
            Timer[F].sleep(ensureSleepTimeBetweenChecks) >> ensureOffline(peerClientMetadata, attempts - 1)
          else true.pure[F]
        case _ => false.pure[F]
      }

  def markOffline(peers: List[PeerData]): F[Unit] =
    peers.traverse { pd =>
      (for {
        _ <- logger.info(s"Marking dead peer: ${pd.peerMetadata.id.short} (${pd.peerMetadata.host}) as offline")
        _ <- unboundedHealthBlocker.blockOn(cluster.markOfflinePeer(pd.peerMetadata.id))
        _ <- metrics.updateMetricAsync("deadPeer", pd.peerMetadata.host)
      } yield ())
        .handleErrorWith(_ => logger.error("Cannot mark peer as offline - error / skipping to next peer if available"))
    }.void
}

object PeerHealthCheck {

  def apply[F[_]: Concurrent: Timer](
    clusterStorage: ClusterStorageAlgebra[F],
    cluster: Cluster[F],
    apiClient: ClientInterpreter[F],
    metrics: Metrics,
    unboundedHealthBlocker: Blocker,
    healthHttpPort: String
  )(
    implicit C: ContextShift[F]
  ) = new PeerHealthCheck[F](clusterStorage, cluster, apiClient, metrics, unboundedHealthBlocker, healthHttpPort = healthHttpPort)

  sealed trait PeerHealthCheckStatus

  object PeerHealthCheckStatus {

    implicit val encodePeerHealthCheckStatus: Encoder[PeerHealthCheckStatus] = deriveEncoder
    implicit val decodePeerHealthCheckStatus: Decoder[PeerHealthCheckStatus] = deriveDecoder
  }

  case class PeerAvailable(id: Id) extends PeerHealthCheckStatus

  case class PeerUnresponsive(id: Id) extends PeerHealthCheckStatus

  case class UnknownPeer(id: Id) extends PeerHealthCheckStatus
}
