package org.constellation.infrastructure.p2p.client

import cats.effect.Concurrent
import org.constellation.consensus.Consensus.{ConsensusDataProposal, SelectedUnionBlock, UnionBlockProposal}
import org.constellation.consensus.RoundDataRemote
import org.constellation.domain.p2p.client.ConsensusClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.http4s.client.Client
import io.circe.generic.auto._
import org.constellation.domain.observation.ObservationEvent
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.Method._

class ConsensusClientInterpreter[F[_]: Concurrent](client: Client[F]) extends ConsensusClientAlgebra[F] {
  import ObservationEvent._

  def participateInNewRound(roundData: RoundDataRemote): PeerResponse[F, Boolean] =
    PeerResponse("block-round/new-round", POST) { req =>
      client.successful(req.withEntity(roundData))
    }

  def addConsensusDataProposal(proposal: ConsensusDataProposal): PeerResponse[F, Boolean] =
    PeerResponse("block-round/proposal", POST) { req =>
      client.successful(req.withEntity(proposal))
    }

  def addUnionBlock(proposal: UnionBlockProposal): PeerResponse[F, Boolean] =
    PeerResponse("block-round/union", POST) { req =>
      client.successful(req.withEntity(proposal))
    }

  def addSelectedUnionBlock(proposal: SelectedUnionBlock): PeerResponse[F, Boolean] =
    PeerResponse("block-round/selected", POST) { req =>
      client.successful(req.withEntity(proposal))
    }
}

object ConsensusClientInterpreter {

  def apply[F[_]: Concurrent](client: Client[F]): ConsensusClientInterpreter[F] =
    new ConsensusClientInterpreter[F](client)
}
