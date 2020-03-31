package org.constellation.domain.p2p.client

import org.constellation.consensus.Consensus.{ConsensusDataProposal, SelectedUnionBlock, UnionBlockProposal}
import org.constellation.consensus.RoundDataRemote
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse

trait ConsensusClientAlgebra[F[_]] {
  def participateInNewRound(roundData: RoundDataRemote): PeerResponse[F, Boolean]

  def addConsensusDataProposal(proposal: ConsensusDataProposal): PeerResponse[F, Boolean]

  def addUnionBlock(proposal: UnionBlockProposal): PeerResponse[F, Boolean]

  def addSelectedUnionBlock(proposal: SelectedUnionBlock): PeerResponse[F, Boolean]
}
