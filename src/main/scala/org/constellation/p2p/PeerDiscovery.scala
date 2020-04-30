package org.constellation.p2p

import cats.data._
import cats.effect.{Concurrent, ContextShift}
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.PeerMetadata
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.schema.Id

class PeerDiscovery[F[_]](apiClient: ClientInterpreter[F], cluster: Cluster[F], ownId: Id)(
  implicit F: Concurrent[F],
  C: ContextShift[F]
) {

  private val logger = Slf4jLogger.getLogger[F]

  val queue: Ref[F, Option[NonEmptyList[PeerClientMetadata]]] = Ref.unsafe(None)

  def discoverFrom(peerMetadata: PeerMetadata): F[Unit] =
    for {
      peers <- apiClient.nodeMetadata
        .getPeers()
        .run(peerMetadata.toPeerClientMetadata)
        .handleErrorWith(_ => F.pure(Seq.empty[PeerMetadata]))

      knownPeers <- cluster.getPeerInfo
      unknownPeers = peers.filter { peer =>
        peer.id != ownId && !knownPeers.exists(_._2.peerMetadata.toPeerClientMetadata == peer.toPeerClientMetadata)
      }.toList

      _ <- unknownPeers.map(_.toPeerClientMetadata).traverse(addNextPeer)

      _ <- hasNextPeer.ifM(
        F.start(C.shift >> joinNextPeer).void,
        F.unit
      )
    } yield ()

  def joinNextPeer: F[Unit] =
    queue.modify {
      case None       => (None, None)
      case Some(list) => (NonEmptyList.fromList(list.tail), Some(list.head))
    } >>= { nextPeer =>
      val join = for {
        peer <- EitherT.fromOption[F](nextPeer, new Throwable("Next peer is empty")).rethrowT
        registrationRequest <- apiClient.sign.getRegistrationRequest().run(peer)
        _ <- cluster.pendingRegistration(peer.host, registrationRequest)
        prr <- cluster.pendingRegistrationRequest
        _ <- apiClient.sign.register(prr).run(peer)
      } yield ()

      if (nextPeer.isDefined) {
        (join >> F.start(C.shift >> joinNextPeer).void).handleErrorWith { err =>
          logger.error(s"Error in joining next peer: ${err.getMessage}")
        }
      } else F.unit
    }

  def hasNextPeer: F[Boolean] =
    queue.get.map(_.nonEmpty)

  private def addNextPeer(pm: PeerClientMetadata): F[Unit] =
    queue.modify { q =>
      val add = q match {
        case None                               => NonEmptyList.one(pm)
        case Some(list) if list.exists(_ == pm) => list
        case Some(list)                         => list.append(pm)
      }

      (add.some, ())
    }

}

object PeerDiscovery {

  def apply[F[_]: Concurrent: ContextShift](apiClient: ClientInterpreter[F], cluster: Cluster[F], ownId: Id) =
    new PeerDiscovery(apiClient, cluster, ownId)
}
