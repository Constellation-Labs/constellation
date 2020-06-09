package org.constellation.p2p

import io.circe.{Decoder, Encoder}
import org.constellation.schema.Id
import org.constellation.ResourceInfo
import io.circe.generic.semiauto._

case class PeerAuthSignRequest(salt: Long)

object PeerAuthSignRequest {
  implicit val peerAuthSignRequestDecoder: Decoder[PeerAuthSignRequest] = deriveDecoder
  implicit val peerAuthSignRequestEncoder: Encoder[PeerAuthSignRequest] = deriveEncoder
}

case class PeerRegistrationRequest(
  host: String,
  port: Int,
  id: Id,
  resourceInfo: ResourceInfo,
  majorityHeight: Option[Long],
  participatesInGenesisFlow: Boolean,
  participatesInRollbackFlow: Boolean,
  joinsAsInitialFacilitator: Boolean,
  whitelistingHash: String
)

object PeerRegistrationRequest {
  implicit val peerRegistrationRequestDecoder: Decoder[PeerRegistrationRequest] = deriveDecoder
  implicit val peerRegistrationRequestEncoder: Encoder[PeerRegistrationRequest] = deriveEncoder
}

case class PeerUnregister(host: String, port: Int, id: Id, majorityHeight: Long)

object PeerUnregister {
  implicit val peerUnregisterDecoder: Decoder[PeerUnregister] = deriveDecoder
  implicit val peerUnregisterEncoder: Encoder[PeerUnregister] = deriveEncoder
}
