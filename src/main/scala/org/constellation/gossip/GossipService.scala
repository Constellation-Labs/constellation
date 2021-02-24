package org.constellation.gossip

import java.security.KeyPair

import cats.Parallel
import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
import cats.effect.syntax.all._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil
import org.constellation.gossip.bisect.bisectA
import org.constellation.gossip.sampling.{GossipPath, PeerSampling}
import org.constellation.gossip.state.{GossipMessage, GossipMessagePathTracker}
import org.constellation.gossip.validation.EndOfCycle
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.Cluster
import org.constellation.schema.Id
import org.constellation.util.Metrics

import scala.concurrent.duration._

abstract class GossipService[F[_]: Parallel, A](
  selfId: Id,
  keyPair: KeyPair,
  peerSampling: PeerSampling[F],
  cluster: Cluster[F],
  metrics: Metrics,
  messageTracker: GossipMessagePathTracker[F, A]
)(
  implicit F: Concurrent[F],
  T: Timer[F]
) {
  protected val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  private val spreadRequestTimeout = ConfigUtil.getDurationFromConfig("gossip.spread-request-timeout", 5 seconds)
  private val recoverThreadLimit = ConfigUtil.getOrElse("gossip.recover-thread-limit", 4L)

  /**
    * How to spread the message to peer
    */
  protected def spreadFn(peerClientMetadata: PeerClientMetadata, message: GossipMessage[A]): F[Unit]

  /**
    * How to check if message was received by peer
    */
  protected def validationFn(peerClientMetadata: PeerClientMetadata, message: GossipMessage[A]): F[Boolean]

  def recover(message: GossipMessage[A]): F[Unit] = {
    val sequence = message.path.toIndexedSeq match {
      case `selfId` +: mid :+ `selfId` => mid
      case _ @seq                      => seq
    }

    for {
      _ <- messageTracker.fail(message.path.id)
      failureNode <- bisectA(
        (id: Id) => getClientMetadata(id).flatMap(validationFn(_, message)),
        sequence
      )
      sequenceToRetry = sequence.dropWhile(node => !failureNode.contains(node))
      clients <- sequenceToRetry.toList.traverse(getClientMetadata)
      _ <- clients.parTraverseN(recoverThreadLimit) { client =>
        generatePathId.flatMap { pathId =>
          spreadFn(
            client,
            message
              .copy(path = GossipPath(IndexedSeq(selfId, client.id, selfId), s"${message.path.id}_$pathId"))
              .sign(keyPair)
          )
        }
      } // TODO: Force trigger peer healthcheck if it fails
      _ <- messageTracker.remove(message.path.id)
    } yield ()
  }

  /**
    * Start spreading the message to fanout
    */
  def spread(data: A): F[Unit] =
    for {
      _ <- logger.debug(s"Starting spreading of data: $data")
      messages <- peerSampling.selectPaths
        .map(_.map(GossipMessage(data, _, selfId)))
      _ <- messages.map(spreadInit).parSequence
    } yield ()

  /**
    * Forward message to next node on path
    */
  def spread(message: GossipMessage[A]): F[Unit] =
    for {
      _ <- logger.debug(
        s"Received rumor on path ${message.path.id}. Passing to next peer on path (${message.path.next.map(_.short)})."
      )
      _ <- signAndForward(message).handleErrorWith(
        e =>
          logger.error(e)(s"Forwarding message on path ${message.path.id} failed")
            >> metrics.incrementMetricAsync(s"gossip_errorSpreadForwardCount")
      )
    } yield ()

  def finishCycle(message: GossipMessage[A]): F[Unit] = messageTracker.success(message.path.id)

  private def signAndForward(message: GossipMessage[A]): F[Unit] =
    message.path.next match {
      case Some(id) => getClientMetadata(id).flatMap(spreadFn(_, message.sign(keyPair)))
      case None     => F.raiseError[Unit](EndOfCycle)
    }

  private def spreadInit(message: GossipMessage[A]): F[Unit] = {
    val round = for {
      _ <- messageTracker.start(message)
      _ <- logger.debug(s"Started path ${message.path.id}: ${message.path.toIndexedSeq.map(_.short).mkString(" -> ")}")
      _ <- metrics.incrementMetricAsync[F]("gossip_spreadInitCount")
      _ <- signAndForward(message)
      _ <- T.sleep(getPathTimeout(message.path))
      succeeded <- messageTracker.isSuccess(message)
      _ <- if (succeeded) {
        messageTracker.remove(message.path.id) >>
          logger.debug(s"Succeeded path ${message.path.id}") >>
          metrics.incrementMetricAsync("gossip_succeededSpreadInitCount")
      } else {
        recover(message) >> metrics.incrementMetricAsync("gossip_failedSpreadInitCount")
      }
    } yield ()

    round.handleErrorWith(
      e =>
        recover(message) >> logger.error(e)(s"Spreading message on path ${message.path.id} failed") >>
          metrics.incrementMetricAsync("gossip_errorSpreadInitCount")
    )
  }

  private def getPathTimeout(path: GossipPath): FiniteDuration =
    path.toIndexedSeq.length * spreadRequestTimeout

  protected def getClientMetadata(id: Id): F[PeerClientMetadata] =
    cluster.getPeerInfo.flatMap(_.get(id) match {
      case Some(metadata) => metadata.peerMetadata.toPeerClientMetadata.pure[F]
      case None           => F.raiseError[PeerClientMetadata](MissingClientForId(id))
    })

  private def generatePathId: F[String] =
    FUUID.randomFUUID.map(_.toString)
}
