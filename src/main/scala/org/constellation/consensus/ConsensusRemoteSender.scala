package org.constellation.consensus

import cats.effect.{Concurrent, ContextShift, LiftIO, Sync}
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.scalalogging.StrictLogging
import org.constellation.PeerMetadata
import org.constellation.consensus.Consensus.{FacilitatorId, RoundData, RoundId}
import org.constellation.consensus.ConsensusManager.{
  BroadcastLightTransactionProposal,
  BroadcastSelectedUnionBlock,
  BroadcastUnionBlockProposal
}
import org.constellation.p2p.PeerData
import org.constellation.primitives.{ChannelMessage, Observation, TipSoe, Transaction}

class ConsensusRemoteSender[F[_]: Concurrent](
  contextShift: ContextShift[F]
) extends StrictLogging {

  def notifyFacilitators(roundData: RoundData): F[List[Response[Unit]]] =
    sendToAll(
      ConsensusRoute.newRoundFullPath,
      roundData.roundId,
      roundData.peers.toList,
      RoundDataRemote(
        roundData.roundId,
        roundData.peers.map(_.peerMetadata),
        roundData.lightPeers.map(_.peerMetadata),
        roundData.facilitatorId,
        roundData.transactions,
        roundData.tipsSOE,
        roundData.messages,
        roundData.observations
      ),
      "NotifyFacilitators"
    )

  def broadcastLightTransactionProposal(cmd: BroadcastLightTransactionProposal): F[Unit] =
    sendToAll(
      ConsensusRoute.proposalFullPath,
      cmd.roundId,
      cmd.peers.toList,
      cmd.transactionsProposal,
      "BroadcastLightTransactionProposal"
    ).void

  def broadcastBlockUnion(cmd: BroadcastUnionBlockProposal): F[Unit] =
    sendToAll(
      ConsensusRoute.unionFullPath,
      cmd.roundId,
      cmd.peers.toList,
      cmd.proposal,
      "BroadcastUnionBlockProposal"
    ).void

  def broadcastSelectedUnionBlock(cmd: BroadcastSelectedUnionBlock): F[Unit] =
    sendToAll(
      ConsensusRoute.selectedFullPath,
      cmd.roundId,
      cmd.peers.toList,
      cmd.cb,
      "BroadcastSelectedUnionBlock"
    ).void

  def sendToAll(
    path: String,
    roundId: RoundId,
    peers: List[PeerData],
    cmd: AnyRef,
    msg: String
  ): F[List[Response[Unit]]] =
    peers.traverse(
      pd =>
        pd.client
          .postNonBlockingUnitF(path, cmd)(contextShift)
          .flatTap(
            r =>
              Sync[F]
                .delay(
                  logger.debug(s"Consensus ${roundId} sending msg ${msg}  code ${r.code} and text ${r.statusText}")
                )
          )
    )

}

case class RoundDataRemote(
  roundId: RoundId,
  peers: Set[PeerMetadata],
  lightPeers: Set[PeerMetadata],
  facilitatorId: FacilitatorId,
  transactions: List[Transaction],
  tipsSOE: TipSoe,
  messages: Seq[ChannelMessage],
  observations: List[Observation]
)
