package org.constellation.gossip

import java.security.KeyPair
import cats.Parallel
import cats.effect.syntax.all._
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.gossip.bisect.Bisect
import org.constellation.gossip.sampling.{GossipPath, PeerSampling}
import org.constellation.gossip.state.{GossipMessage, GossipMessagePathTracker}
import org.constellation.gossip.validation.EndOfCycle
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.Cluster
import org.constellation.schema.Id
import org.constellation.util.Metrics
import org.constellation.collection.SeqUtils.PathSeqOps

import scala.concurrent.duration._

abstract class GossipService[F[_]: Parallel, A](
  selfId: Id,
  keyPair: KeyPair,
  peerSampling: PeerSampling[F],
  clusterStorage: ClusterStorageAlgebra[F],
  metrics: Metrics,
  messageTracker: GossipMessagePathTracker[F, A]
)(
  implicit F: Concurrent[F],
  C: ContextShift[F],
  T: Timer[F]
) {
  protected val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  private val bisect = Bisect[F]
  private val spreadRequestTimeout = ConfigUtil.getDurationFromConfig("gossip.spread-request-timeout", 5 seconds)
  private val spreadThreadLimit = ConfigUtil.getOrElse("gossip.spread-thread-limit", 10L)

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
      _ <- logger.debug(s"Recovering path ${message.path.id}")
      failureNode <- bisect.runA(
        (id: Id) => getClientMetadata(id).flatMap(validationFn(_, message)),
        sequence
      )
      retryIds = sequence.dropWhile(node => !failureNode.contains(node))
      _ <- logger.debug(s"Retrying path ${message.path.id} on nodes ${retryIds.formatPath}")
      _ <- retryIds.headOption.traverse { headId =>
        F.start(C.shift >> retryHead(headId, message))
      }
      _ <- if (retryIds.tail.isEmpty)
        F.unit
      else
        F.start(C.shift >> retryTail(retryIds.tail, message))
    } yield ()
  }

  // This method uses spreadFn instead of spreadInit to avoid infinite recovery loops
  private def retryHead(headId: Id, message: GossipMessage[A]): F[Unit] =
    for {
      client <- getClientMetadata(headId)
      headPathId = s"retryHead_${message.path.id}"
      ids = selfId +: headId +: selfId +: IndexedSeq.empty
      newMessage = message.copy(path = GossipPath(ids, headPathId)).sign(keyPair)
      _ <- spreadFn(client, newMessage).handleErrorWith { e =>
        logger.error(e)(s"Error retrying bisect head $headPathId")
      }
    } yield ()

  private def retryTail(tailIds: IndexedSeq[Id], message: GossipMessage[A]): F[Unit] =
    spreadInit(message.copy(path = GossipPath(selfId +: tailIds :+ selfId, s"retryTail_${message.path.id}")))

  /**
    * Start spreading the message to fanout
    */
  def spread(data: A): F[Unit] =
    for {
      _ <- logger.debug(s"Starting spreading of data: $data")
      messages <- peerSampling.selectPaths
        .map(_.map(GossipMessage(data, _, selfId)))
      _ <- messages.parTraverseN(spreadThreadLimit)(spreadInit)
    } yield ()

  /**
    * Forward message to next node on path
    */
  def spread(message: GossipMessage[A]): F[Unit] =
    for {
      _ <- logger.debug(
        s"Received rumor on path ${message.path.id}. Passing to next peer on path (${message.path.next.map(_.hex)})."
      )
      _ <- signAndForward(message).handleErrorWith { e =>
        logger.error(e)(s"Error forwarding message on path ${message.path.id}") >>
          metrics.incrementMetricAsync(s"gossip_errorSpreadForwardCount")
      }
    } yield ()

  def finishCycle(message: GossipMessage[A]): F[Unit] = messageTracker.success(message.path.id)

  private def signAndForward(message: GossipMessage[A]): F[Unit] =
    message.path.next match {
      case Some(id) => getClientMetadata(id).flatMap(spreadFn(_, message.sign(keyPair)))
      case None     => F.raiseError[Unit](EndOfCycle)
    }

  private def spreadInit(message: GossipMessage[A]): F[Unit] = {
    val roundResult = for {
      _ <- messageTracker.start(message)
      _ <- logger.debug(s"Started path ${message.path.id}: ${message.path.toIndexedSeq.map(_.short).mkString(" -> ")}")
      _ <- metrics.incrementMetricAsync[F]("gossip_spreadInitCount")
      _ <- signAndForward(message)
      _ <- T.sleep(getPathTimeout(message.path))
      succeeded <- messageTracker.isSuccess(message)
      _ <- if (succeeded) {
        logger.debug(s"Succeeded path ${message.path.id}") >>
          metrics.incrementMetricAsync("gossip_succeededSpreadInitCount")
      } else {
        logger.debug(s"Failed path ${message.path.id}") >>
          metrics.incrementMetricAsync("gossip_failedSpreadInitCount")
      }
    } yield succeeded

    roundResult.handleErrorWith { e =>
      logger.error(e)(s"Error spreading message on path ${message.path.id}") >>
        metrics.incrementMetricAsync("gossip_errorSpreadInitCount") >> false.pure[F]
    }.ifM(
      messageTracker.remove(message.path.id),
      messageTracker.remove(message.path.id) >> recover(message)
    )
  }

  private def getPathTimeout(path: GossipPath): FiniteDuration =
    path.toIndexedSeq.length * spreadRequestTimeout

  protected def getClientMetadata(id: Id): F[PeerClientMetadata] =
    clusterStorage.getPeers.flatMap(_.get(id) match {
      case Some(metadata) => metadata.peerMetadata.toPeerClientMetadata.pure[F]
      case None           => F.raiseError[PeerClientMetadata](MissingClientForId(id))
    })
}
