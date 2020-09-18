package org.constellation.schema

import java.time.LocalDateTime

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.constellation.schema.signature.Signable

case class PeerNotification(id: Id, state: PeerState, timestamp: LocalDateTime = LocalDateTime.now()) extends Signable

object PeerNotification {
  implicit val peerNotificationEncoder: Encoder[PeerNotification] = deriveEncoder
  implicit val peerNotificationDecoder: Decoder[PeerNotification] = deriveDecoder
}
