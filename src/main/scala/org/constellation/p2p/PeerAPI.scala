package org.constellation.p2p

import org.constellation.schema.Id
import org.constellation.ResourceInfo
case class PeerAuthSignRequest(salt: Long)

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

case class PeerUnregister(host: String, port: Int, id: Id, majorityHeight: Long)
