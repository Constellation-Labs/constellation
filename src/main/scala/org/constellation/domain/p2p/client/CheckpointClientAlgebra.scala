package org.constellation.domain.p2p.client

import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.v2.GenesisObservation
import org.constellation.schema.v2.checkpoint.{CheckpointCache, FinishedCheckpoint}
import org.constellation.schema.v2.signature.{SignatureRequest, SignatureResponse}

trait CheckpointClientAlgebra[F[_]] {
  def getGenesis(): PeerResponse[F, Option[GenesisObservation]]

  def getCheckpoint(hash: String): PeerResponse[F, Option[CheckpointCache]]

  def requestBlockSignature(signatureRequest: SignatureRequest): PeerResponse[F, SignatureResponse]

  def sendFinishedCheckpoint(checkpoint: FinishedCheckpoint): PeerResponse[F, Boolean]
}
