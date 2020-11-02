package org.constellation.gossip

import cats.Parallel
import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.gossip.bisect.bisectA
import org.constellation.gossip.sampling.PeerSampling
import org.constellation.gossip.state.{GossipMessage, GossipMessagePathTracker}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.Cluster
import org.constellation.schema.Id

import scala.concurrent.duration._

abstract class GossipService[F[_]: Parallel, A](
  selfId: Id,
  peerSampling: PeerSampling[F],
  cluster: Cluster[F],
  messageTracker: GossipMessagePathTracker[F, A]
)(
  implicit F: Concurrent[F],
  T: Timer[F]
) {
  protected val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

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
      _ <- clients.traverse(spreadFn(_, message)) // If it fails we should force trigger healthcheck
      _ <- messageTracker.remove(message.path.id)
    } yield ()
  }

  /**
    * Start spreading the message to fanout
    */
  def spread(data: A, fanout: Int): F[Unit] =
    for {
      _ <- logger.debug(s"Starting spreading of data: $data with fanout $fanout")
      messages <- peerSampling.selectPaths
        .map(_.map(GossipMessage(data, _)))
      _ <- messages.map { msg =>
        for {
          _ <- logger.debug(s"Path ${msg.path.id}: ${msg.path.toIndexedSeq.map(_.short).mkString(" -> ")}")
          _ <- messageTracker.start(msg)
          _ <- spreadToNext(msg).handleErrorWith(_ => recover(msg))
          _ <- logger.debug(s"Started path ${msg.path.id}")
          _ <- T.sleep(30 seconds)
          succeeded <- messageTracker.isSuccess(msg)
          _ <- if (succeeded) {
            messageTracker.remove(msg.path.id) >> logger.debug(s"Succeeded path ${msg.path.id}")
          } else {
            recover(msg)
          }
        } yield ()
      }.parSequence
    } yield ()

  def validate(message: GossipMessage[A], senderId: Id): Either[GossipError, GossipMessage[A]] =
    if (!couldSend(message, senderId)) {
      Left(IncorrectSenderId(senderId))
    } else if (!couldReceive(message)) {
      Left(IncorrectReceiverId(selfId.some, message.path.next))
    } else if (isEndOfCycle(message)) {
      Left(EndOfCycle)
    } else Right(message)

  def finishCycle(message: GossipMessage[A]): F[Unit] = messageTracker.success(message.path.id)

  /**
    * Forward message to next node on path
    */
  def spread(message: GossipMessage[A]): F[Unit] =
    for {
      _ <- logger.debug(
        s"Received rumor on path ${message.path.id}. Passing to next peer on path (${message.path.next})."
      )
      _ <- spreadToNext(GossipMessage(message.data, message.path.accept))
    } yield ()

  private def couldSend(message: GossipMessage[A], senderId: Id): Boolean =
    message.path.isCurrent(senderId)

  private def couldReceive(message: GossipMessage[A]): Boolean =
    message.path.isNext(selfId)

  private def isEndOfCycle(message: GossipMessage[A]): Boolean =
    message.path.isNext(selfId) && message.path.isFirst(selfId) && message.path.isLast(selfId)

  private def spreadToNext(message: GossipMessage[A]): F[Unit] =
    message.path.next match {
      case Some(id) => getClientMetadata(id).flatMap(spreadFn(_, message))
      case None     => throw EndOfCycle
    }

  protected def getClientMetadata(id: Id): F[PeerClientMetadata] =
    cluster.getPeerInfo.map(_.get(id) match {
      case Some(metadata) => metadata.peerMetadata.toPeerClientMetadata
      case None           => throw MissingClientForId(id)
    })
}
