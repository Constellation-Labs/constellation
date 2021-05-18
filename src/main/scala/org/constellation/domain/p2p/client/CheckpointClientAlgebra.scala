package org.constellation.domain.p2p.client

import org.constellation.gossip.state.GossipMessage
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.GenesisObservation
import org.constellation.schema.checkpoint.{CheckpointBlockPayload, CheckpointCache, FinishedCheckpoint}
import org.constellation.schema.signature.{SignatureRequest, SignatureResponse}

trait CheckpointClientAlgebra[F[_]] {
  def getGenesis(): PeerResponse[F, Option[GenesisObservation]]

  def getCheckpoint(hash: String): PeerResponse[F, Option[CheckpointCache]]

  def checkFinishedCheckpoint(hash: String): PeerResponse[F, Option[CheckpointCache]]

  def requestBlockSignature(signatureRequest: SignatureRequest): PeerResponse[F, SignatureResponse]

  def postFinishedCheckpoint(message: GossipMessage[CheckpointBlockPayload]): PeerResponse[F, Unit]
}
