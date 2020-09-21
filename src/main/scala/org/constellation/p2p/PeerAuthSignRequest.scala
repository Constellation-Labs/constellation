package org.constellation.p2p

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class PeerAuthSignRequest(salt: Long)

object PeerAuthSignRequest {
  implicit val peerAuthSignRequestDecoder: Decoder[PeerAuthSignRequest] = deriveDecoder
  implicit val peerAuthSignRequestEncoder: Encoder[PeerAuthSignRequest] = deriveEncoder
}
