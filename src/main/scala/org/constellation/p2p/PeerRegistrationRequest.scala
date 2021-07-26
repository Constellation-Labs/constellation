package org.constellation.p2p

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.ResourceInfo
import org.constellation.schema.{Id, NodeType}
import org.constellation.session.SessionTokenService.Token

case class PeerRegistrationRequest(
  host: String,
  port: Int,
  id: Id,
  resourceInfo: ResourceInfo,
  majorityHeight: Option[Long],
  participatesInGenesisFlow: Boolean,
  participatesInRollbackFlow: Boolean,
  joinsAsInitialFacilitator: Boolean,
  whitelistingHash: String,
  token: Token,
  nodeType: NodeType,
  isReconciliationJoin: Boolean
)

object PeerRegistrationRequest {
  implicit val peerRegistrationRequestDecoder: Decoder[PeerRegistrationRequest] = deriveDecoder
  implicit val peerRegistrationRequestEncoder: Encoder[PeerRegistrationRequest] = deriveEncoder
}
