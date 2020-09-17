package org.constellation.util

import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.schema.Id

case class PeerApiClient(id: Id, client: PeerClientMetadata)
