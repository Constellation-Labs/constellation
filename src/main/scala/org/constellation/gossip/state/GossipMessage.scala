package org.constellation.gossip.state

import java.security.KeyPair

import io.circe._
import io.circe.generic.semiauto._
import org.constellation.gossip.sampling.GossipPath
import org.constellation.schema.Id
import org.constellation.schema.signature.{HashSignature, SignHelp, Signable}

case class GossipMessage[A](
  data: A,
  path: GossipPath,
  origin: Id,
  signatures: IndexedSeq[HashSignature] = IndexedSeq.empty
) extends Signable {

  override def trackRefs: Boolean = false

  def sign(kp: KeyPair): GossipMessage[A] = {
    val signature = SignHelp.hashSign(hash, kp)
    this.copy(
      path = path.forward,
      signatures = signatures ++ IndexedSeq(signature)
    )
  }
}

object GossipMessage {
  implicit def gossipMessageEncoder[A: Encoder]: Encoder[GossipMessage[A]] = deriveEncoder
  implicit def gossipMessageDecoder[A: Decoder]: Decoder[GossipMessage[A]] = deriveDecoder
}
