package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.consensus.{FinishedCheckpoint, SignatureRequest, SignatureResponse}
import org.constellation.domain.p2p.client.CheckpointClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.primitives.Schema.{CheckpointCache, GenesisObservation}
import org.http4s.client.Client
import org.constellation.domain.observation.ObservationEvent
import org.constellation.session.SessionTokenService
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.Method._
import scala.language.reflectiveCalls

class CheckpointClientInterpreter[F[_]: Concurrent: ContextShift](client: Client[F], sessionTokenService: SessionTokenService[F])
    extends CheckpointClientAlgebra[F] {
  import ObservationEvent._
  import GenesisObservation._
  import CheckpointCache._
  import SignatureResponse._
  import FinishedCheckpoint._

  def getGenesis(): PeerResponse[F, Option[GenesisObservation]] =
    PeerResponse[F, Option[GenesisObservation]]("genesis")(client, sessionTokenService)

  def getCheckpoint(hash: String): PeerResponse[F, Option[CheckpointCache]] =
    PeerResponse[F, Option[CheckpointCache]](s"checkpoint/$hash")(client, sessionTokenService)

  def requestBlockSignature(signatureRequest: SignatureRequest): PeerResponse[F, SignatureResponse] =
    PeerResponse(s"request/signature", POST)(client, sessionTokenService) { (req, c) =>
      c.expect[SignatureResponse](req.withEntity(signatureRequest))
    }

  def sendFinishedCheckpoint(checkpoint: FinishedCheckpoint): PeerResponse[F, Boolean] =
    PeerResponse(s"finished/checkpoint", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(checkpoint))
    }
}

object CheckpointClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](client: Client[F], sessionTokenService: SessionTokenService[F]): CheckpointClientInterpreter[F] =
    new CheckpointClientInterpreter[F](client, sessionTokenService)
}
