package org.constellation.consensus

import constellation._
import org.constellation.consensus.CrossTalkConsensus.NotifyFacilitators
import org.constellation.consensus.RoundManager.{
  BroadcastLightTransactionProposal,
  BroadcastSelectedUnionBlock,
  BroadcastUnionBlockProposal
}
import org.constellation.p2p.PeerData
import org.constellation.p2p.routes.BlockBuildingRoundRoute
import org.constellation.primitives.Schema.SignedObservationEdge
import org.constellation.primitives.{ChannelMessage, TipSoe, Transaction}
import org.constellation.{DAO, PeerMetadata}

case class RoundDataRemote(
  roundId: RoundId,
  peers: Set[PeerMetadata],
  lightPeers: Set[PeerMetadata],
  facilitatorId: FacilitatorId,
  transactions: List[Transaction],
  tipsSOE: TipSoe,
  messages: Seq[ChannelMessage]
)

class HTTPNodeRemoteSender(implicit val dao: DAO) extends NodeRemoteSender {
  override def notifyFacilitators(cmd: NotifyFacilitators): Unit = {
    val r = cmd.roundData
    parallelFireForget(
      BlockBuildingRoundRoute.newRoundFullPath,
      r.roundId,
      cmd.roundData.peers,
      RoundDataRemote(
        r.roundId,
        r.peers.map(_.peerMetadata),
        r.lightPeers.map(_.peerMetadata),
        r.facilitatorId,
        r.transactions,
        r.tipsSOE,
        r.messages
      ),
      "NotifyFacilitators"
    )
  }

  override def broadcastLightTransactionProposal(cmd: BroadcastLightTransactionProposal): Unit =
    parallelFireForget(
      BlockBuildingRoundRoute.proposalFullPath,
      cmd.roundId,
      cmd.peers,
      cmd.transactionsProposal,
      "BroadcastLightTransactionProposal"
    )

  override def broadcastBlockUnion(cmd: BroadcastUnionBlockProposal): Unit =
    parallelFireForget(
      BlockBuildingRoundRoute.unionFullPath,
      cmd.roundId,
      cmd.peers,
      cmd.proposal,
      "BroadcastUnionBlockProposal"
    )

  override def broadcastSelectedUnionBlock(cmd: BroadcastSelectedUnionBlock): Unit =
    parallelFireForget(
      BlockBuildingRoundRoute.selectedFullPath,
      cmd.roundId,
      cmd.peers,
      cmd.cb,
      "BroadcastSelectedUnionBlock"
    )

  def parallelFireForget(path: String, roundId: RoundId, peers: Iterable[PeerData], cmd: AnyRef, msg: String): Unit =
    peers.par.foreach(_.client.postNonBlockingUnit(path, cmd))
}
