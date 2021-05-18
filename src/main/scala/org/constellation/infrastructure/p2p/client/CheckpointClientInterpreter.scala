package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.p2p.client.CheckpointClientAlgebra
import org.constellation.gossip.state.GossipMessage
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.GenesisObservation
import org.constellation.schema.checkpoint.{CheckpointBlockPayload, CheckpointCache, FinishedCheckpoint}
import org.constellation.schema.signature.{SignatureRequest, SignatureResponse}
import org.constellation.session.SessionTokenService
import org.http4s.Method._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client

import scala.language.reflectiveCalls

class CheckpointClientInterpreter[F[_]: ContextShift](
  client: Client[F],
  sessionTokenService: SessionTokenService[F]
)(implicit F: Concurrent[F])
    extends CheckpointClientAlgebra[F] {
  import CheckpointCache._
  import FinishedCheckpoint._
  import GenesisObservation._
  import SignatureResponse._

  def getGenesis(): PeerResponse[F, Option[GenesisObservation]] =
    PeerResponse[F, Option[GenesisObservation]]("genesis")(client, sessionTokenService)

  def getCheckpoint(hash: String): PeerResponse[F, Option[CheckpointCache]] =
    PeerResponse[F, Option[CheckpointCache]](s"checkpoint/$hash")(client, sessionTokenService)

  def checkFinishedCheckpoint(hash: String): PeerResponse[F, Option[CheckpointCache]] =
    PeerResponse[F, Option[CheckpointCache]](s"checkpoint/$hash/check")(client, sessionTokenService)

  def requestBlockSignature(signatureRequest: SignatureRequest): PeerResponse[F, SignatureResponse] =
    PeerResponse(s"request/signature", POST)(client, sessionTokenService) { (req, c) =>
      c.expect[SignatureResponse](req.withEntity(signatureRequest))
    }

  def postFinishedCheckpoint(message: GossipMessage[CheckpointBlockPayload]): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean](s"peer/checkpoint/finished", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(message))
    }.flatMapF { a =>
      if (a) F.unit
      else
        F.raiseError(
          new Throwable(
            s"Cannot post finished checkpoint of hash ${message.payload.block.value.hash}"
          )
        )
    }

  def postFinishedCheckpoint(message: GossipMessage[CheckpointBlockPayload]): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean](s"peer/checkpoint/finished", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(message))
    }.flatMapF { a =>
      if (a) F.unit
      else
        F.raiseError(
          new Throwable(
            s"Cannot post finished checkpoint of hash ${message.payload.block.value.hash}"
          )
        )
    }
}

object CheckpointClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): CheckpointClientInterpreter[F] =
    new CheckpointClientInterpreter[F](client, sessionTokenService)
}
