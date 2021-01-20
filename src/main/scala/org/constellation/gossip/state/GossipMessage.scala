package org.constellation.gossip.state

import java.security.KeyPair

import cats.effect.Sync
import org.constellation.gossip.sampling.GossipPath
import org.constellation.infrastructure.p2p.BinaryCodec
import org.constellation.schema.Id
import org.constellation.schema.signature.{HashSignature, SignHelp, Signable}
import org.http4s.{EntityDecoder, EntityEncoder}

case class GossipMessage[A](
  payload: A,
  path: GossipPath,
  origin: Id,
  signatures: IndexedSeq[HashSignature] = IndexedSeq.empty
) extends Signable {

  override def serializeWithRefs: Boolean = false

  def sign(kp: KeyPair): GossipMessage[A] = {
    val signature = SignHelp.hashSign(hash, kp)
    this.copy(
      path = path.forward,
      signatures = signatures ++ IndexedSeq(signature)
    )
  }
}

object GossipMessage {
  implicit def gossipMessageEncoder[F[_], A]: EntityEncoder[F, GossipMessage[A]] =
    BinaryCodec.encoder[F, GossipMessage[A]]

  implicit def gossipMessageDecoder[F[_]: Sync, A]: EntityDecoder[F, GossipMessage[A]] =
    BinaryCodec.decoder[F, GossipMessage[A]]
}
