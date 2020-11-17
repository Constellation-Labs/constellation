package org.constellation.gossip

import java.security.KeyPair

import cats.Parallel
import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
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

import scala.concurrent.duration._

abstract class GossipService[F[_]: Parallel, A](
  selfId: Id,
  keyPair: KeyPair,
  peerSampling: PeerSampling[F],
  cluster: Cluster[F],
  messageTracker: GossipMessagePathTracker[F, A]
)(
  implicit F: Concurrent[F],
  T: Timer[F]
) {
  protected val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  private val spreadRequestTimeout = ConfigUtil.getDurationFromConfig("gossip.spread-request-timeout", 10 seconds)

  /**
    * How to spread the message to peer
    */
  protected def spreadFn(peerClientMetadata: PeerClientMetadata, message: GossipMessage[A]): F[Unit]

  /**
    * How to check if message was received by peer
    */
  protected def validationFn(peerClientMetadata: PeerClientMetadata, message: GossipMessage[A]): F[Boolean]

  def recover(message: GossipMessage[A]): F[Unit] = {
    val sequence = message.path.toIndexedSeq

    for {
      _ <- messageTracker.fail(message.path.id)
      failureNode <- bisectA(
        (id: Id) => getClientMetadata(id).flatMap(validationFn(_, message))
      )(sequence)
      sequenceToRetry = sequence.dropWhile(failureNode.contains)
      clients <- sequenceToRetry.toList.traverse(getClientMetadata)
      _ <- clients.traverse(spreadFn(_, message)) // TODO: Force trigger peer healthcheck if it fails
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
        e => logger.error(e)(s"Forwarding message on path ${message.path.id} failed")
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
      _ <- signAndForward(message)
      _ <- T.sleep(getPathTimeout(message.path))
      succeeded <- messageTracker.isSuccess(message)
      _ <- if (succeeded) {
        messageTracker.remove(message.path.id) >> logger.debug(s"Succeeded path ${message.path.id}")
      } else {
        recover(message)
      }
    } yield ()

    round.handleErrorWith(
      e => logger.error(e)(s"Spreading message on path ${message.path.id} failed") >> recover(message)
    )
  }

  private def getPathTimeout(path: GossipPath): FiniteDuration =
    path.toIndexedSeq.length * spreadRequestTimeout

  protected def getClientMetadata(id: Id): F[PeerClientMetadata] =
    cluster.getPeerInfo.flatMap(_.get(id) match {
      case Some(metadata) => metadata.peerMetadata.toPeerClientMetadata.pure[F]
      case None           => F.raiseError[PeerClientMetadata](MissingClientForId(id))
    })
}
