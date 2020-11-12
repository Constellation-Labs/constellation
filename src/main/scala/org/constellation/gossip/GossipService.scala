package org.constellation.gossip

import cats.data.OptionT
import cats.effect.Concurrent
import cats.implicits._
import org.constellation.gossip.sampling.PeerSampling
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.Cluster
import org.constellation.schema.v2.{Id, NodeState}

abstract class GossipService[F[_], A](selfId: Id, peerSampling: PeerSampling[F, Id], cluster: Cluster[F])(
  implicit val F: Concurrent[F]
) {

  protected def spread(data: A, fanout: Int)(spreadFn: (PeerClientMetadata, GossipMessage[A]) => F[Unit]): F[Unit] =
    for {
      messages <- peerSampling
        .selectPaths(fanout)
        .map(_.map(GossipPath(_)).map(GossipMessage(data, _)))

      _ <- messages.toList.traverse(spreadToNext(_)(spreadFn))
    } yield ()

  protected def spread(
    message: GossipMessage[A]
  )(spreadFn: (PeerClientMetadata, GossipMessage[A]) => F[Unit]): F[Unit] = {
    val couldReceive = message.path.isNext(selfId)
    if (!couldReceive) {
      F.raiseError(IncorrectNextNodeOnPath(selfId.some, message.path.next))
    } else {
      spreadToNext(GossipMessage(message.data, message.path.accept))(spreadFn)
    }
  }

  private def spreadToNext(
    message: GossipMessage[A]
  )(spreadFn: (PeerClientMetadata, GossipMessage[A]) => F[Unit]): F[Unit] =
    message.path.next
      .toOptionT[F]
      .flatMap(id => OptionT(getClientMetadata(id)))
      .cataF(F.raiseError(EndOfPath).void, spreadFn(_, message))

  private def getClientMetadata(id: Id): F[Option[PeerClientMetadata]] =
    cluster.getPeerInfo
      .map(_.get(id).map(_.peerMetadata.toPeerClientMetadata))
}
