package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.consensus.{FinishedCheckpoint, SignatureRequest, SignatureResponse}
import org.constellation.domain.p2p.client.CheckpointClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.primitives.Schema.{CheckpointCache, GenesisObservation}
import org.http4s.client.Client
import io.circe.generic.auto._
import org.constellation.domain.observation.ObservationEvent
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.Method._

class CheckpointClientInterpreter[F[_]: Concurrent: ContextShift](client: Client[F])
    extends CheckpointClientAlgebra[F] {
  import ObservationEvent._

  def getGenesis(): PeerResponse[F, Option[GenesisObservation]] =
    PeerResponse[F, Option[GenesisObservation]]("genesis")(client)

  def getCheckpoint(hash: String): PeerResponse[F, Option[CheckpointCache]] =
    PeerResponse[F, Option[CheckpointCache]](s"checkpoint/$hash")(client)

  def requestBlockSignature(signatureRequest: SignatureRequest): PeerResponse[F, SignatureResponse] =
    PeerResponse(s"request/signature", client, POST) { (req, c) =>
      c.expect[SignatureResponse](req.withEntity(signatureRequest))
    }

  def sendFinishedCheckpoint(checkpoint: FinishedCheckpoint): PeerResponse[F, Boolean] =
    PeerResponse(s"finished/checkpoint", client, POST) { (req, c) =>
      c.successful(req.withEntity(checkpoint))
    }
}

object CheckpointClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](client: Client[F]): CheckpointClientInterpreter[F] =
    new CheckpointClientInterpreter[F](client)
}
