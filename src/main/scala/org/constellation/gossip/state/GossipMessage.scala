package org.constellation.gossip.state

import io.circe._
import io.circe.generic.semiauto._
import org.constellation.gossip.sampling.GossipPath

case class GossipMessage[A](data: A, path: GossipPath)

object GossipMessage {
  implicit def gossipMessageEncoder[A: Encoder]: Encoder[GossipMessage[A]] = deriveEncoder
  implicit def gossipMessageDecoder[A: Decoder]: Decoder[GossipMessage[A]] = deriveDecoder
}
