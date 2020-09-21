package org.constellation.p2p

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.schema.Id

case class PeerUnregister(host: String, port: Int, id: Id, majorityHeight: Long)

object PeerUnregister {
  implicit val peerUnregisterDecoder: Decoder[PeerUnregister] = deriveDecoder
  implicit val peerUnregisterEncoder: Encoder[PeerUnregister] = deriveEncoder
}
