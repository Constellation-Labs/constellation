package org.constellation.consensus

import java.net.SocketTimeoutException
import java.security.KeyPair

import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import com.softwaremill.sttp.Response
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.PeerMetadata
import org.constellation.consensus.Consensus.{FacilitatorId, RoundData, RoundId}
import org.constellation.consensus.ConsensusManager.{
  BroadcastConsensusDataProposal,
  BroadcastSelectedUnionBlock,
  BroadcastUnionBlockProposal
}
import org.constellation.domain.observation.{Observation, ObservationService, RequestTimeoutOnConsensus}
import org.constellation.p2p.PeerData
import org.constellation.primitives.{ChannelMessage, TipSoe, Transaction}

class ConsensusRemoteSender[F[_]: Concurrent](
  contextShift: ContextShift[F],
  observationService: ObservationService[F],
  keyPair: KeyPair
) {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

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

  def broadcastConsensusDataProposal(cmd: BroadcastConsensusDataProposal): F[Unit] =
    sendToAll(
      ConsensusRoute.proposalFullPath,
      cmd.roundId,
      cmd.peers.toList,
      cmd.consensusDataProposal,
      "BroadcastConsensusDataProposal"
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
          .onError {
            case _: SocketTimeoutException =>
              observationService
                .put(Observation.create(pd.peerMetadata.id, RequestTimeoutOnConsensus(roundId))(keyPair))
                .void
          }
          .flatTap(
            r => logger.debug(s"Consensus ${roundId} sending msg ${msg}  code ${r.code} and text ${r.statusText}")
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
