package org.constellation.consensus

import constellation._
import org.constellation.consensus.CrossTalkConsensus.NotifyFacilitators
import org.constellation.consensus.RoundManager.{BroadcastLightTransactionProposal, BroadcastSelectedUnionBlock, BroadcastUnionBlockProposal}
import org.constellation.p2p.routes.BlockBuildingRoundRoute
import org.constellation.primitives.Schema.SignedObservationEdge
import org.constellation.primitives.{ChannelMessage, PeerData, Transaction}
import org.constellation.{DAO, PeerMetadata}

case class RoundDataRemote(
  roundId: RoundId,
  peers: Set[PeerMetadata],
  lightPeers: Set[PeerMetadata],
  facilitatorId: FacilitatorId,
  transactions: Seq[Transaction],
  tipsSOE: Seq[SignedObservationEdge],
  messages: Seq[ChannelMessage]
)

class HTTPNodeRemoteSender(implicit val dao: DAO) extends NodeRemoteSender {
  override def notifyFacilitators(cmd: NotifyFacilitators): Unit = {
    val r = cmd.roundData
    parallelFireForget(
      BlockBuildingRoundRoute.newRoundFullPath,
      cmd.roundData.peers,
      RoundDataRemote(
        r.roundId,
        r.peers.map(_.peerMetadata),
        r.lightPeers.map(_.peerMetadata),
        r.facilitatorId,
        r.transactions,
        r.tipsSOE,
        r.messages
      )
    )
  }

  override def broadcastLightTransactionProposal(cmd: BroadcastLightTransactionProposal): Unit =
    parallelFireForget(
      BlockBuildingRoundRoute.proposalFullPath,
      cmd.peers,
      cmd.transactionsProposal
    )

  override def broadcastBlockUnion(cmd: BroadcastUnionBlockProposal): Unit =
    parallelFireForget(BlockBuildingRoundRoute.unionFullPath, cmd.peers, cmd.proposal)

  override def broadcastSelectedUnionBlock(cmd: BroadcastSelectedUnionBlock): Unit =
    parallelFireForget(BlockBuildingRoundRoute.selectedFullPath, cmd.peers, cmd.cb)

  def parallelFireForget(path: String, peers: Iterable[PeerData], cmd: AnyRef): Unit =
    peers.par.foreach(_.client.postNonBlockingUnit(path, cmd))
}
