package org.constellation.domain.p2p.client

import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.p2p.{PeerAuthSignRequest, PeerRegistrationRequest}
import org.constellation.schema.signature.SingleHashSignature

trait SignClientAlgebra[F[_]] {
  def sign(req: PeerAuthSignRequest): PeerResponse[F, SingleHashSignature]

  def register(req: PeerRegistrationRequest): PeerResponse[F, Unit]

  def getRegistrationRequest(): PeerResponse[F, PeerRegistrationRequest]
}
