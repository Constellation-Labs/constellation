package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.consensus.Consensus.{ConsensusDataProposal, SelectedUnionBlock, UnionBlockProposal}
import org.constellation.consensus.RoundDataRemote
import org.constellation.domain.p2p.client.ConsensusClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.v2.observation.ObservationEvent
import org.http4s.client.Client
import org.constellation.session.SessionTokenService
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.Method._

import scala.language.reflectiveCalls

class ConsensusClientInterpreter[F[_]: Concurrent: ContextShift](
  client: Client[F],
  sessionTokenService: SessionTokenService[F]
) extends ConsensusClientAlgebra[F] {
  import ObservationEvent._
  import RoundDataRemote._

  def participateInNewRound(roundData: RoundDataRemote): PeerResponse[F, Boolean] =
    PeerResponse("block-round/new-round", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(roundData))
    }

  def addConsensusDataProposal(proposal: ConsensusDataProposal): PeerResponse[F, Boolean] =
    PeerResponse("block-round/proposal", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(proposal))
    }

  def addUnionBlock(proposal: UnionBlockProposal): PeerResponse[F, Boolean] =
    PeerResponse("block-round/union", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(proposal))
    }

  def addSelectedUnionBlock(proposal: SelectedUnionBlock): PeerResponse[F, Boolean] =
    PeerResponse("block-round/selected", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(proposal))
    }
}

object ConsensusClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): ConsensusClientInterpreter[F] =
    new ConsensusClientInterpreter[F](client, sessionTokenService)
}
