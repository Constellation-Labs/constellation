package org.constellation.consensus

import java.net.SocketTimeoutException
import java.security.KeyPair

import cats.data.NonEmptyList
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import io.circe.generic.semiauto._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Decoder, Encoder}
import org.constellation.PeerMetadata
import org.constellation.consensus.Consensus.{FacilitatorId, RoundData, RoundId}
import org.constellation.consensus.ConsensusManager.{
  BroadcastConsensusDataProposal,
  BroadcastSelectedUnionBlock,
  BroadcastUnionBlockProposal
}
import org.constellation.domain.observation.{Observation, ObservationService, RequestTimeoutOnConsensus}
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.{MajorityHeight, PeerData}
import org.constellation.primitives.{ChannelMessage, TipSoe, Transaction}

class ConsensusRemoteSender[F[_]: Concurrent](
  contextShift: ContextShift[F],
  observationService: ObservationService[F],
  apiClient: ClientInterpreter[F],
  keyPair: KeyPair
) {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def notifyFacilitators(roundData: RoundData): F[List[Boolean]] =
    sendToAll(
      apiClient.consensus
        .participateInNewRound(
          RoundDataRemote(
            roundData.roundId,
            roundData.peers.map(pd => (pd.peerMetadata, pd.majorityHeight)),
            roundData.lightPeers.map(pd => (pd.peerMetadata, pd.majorityHeight)),
            roundData.facilitatorId,
            roundData.transactions,
            roundData.tipsSOE,
            roundData.messages,
            roundData.observations
          )
        )
        .run,
      roundData.roundId,
      roundData.peers.toList,
      "NotifyFacilitators"
    )

  def broadcastConsensusDataProposal(cmd: BroadcastConsensusDataProposal): F[Unit] =
    sendToAll(
      apiClient.consensus.addConsensusDataProposal(cmd.consensusDataProposal).run,
      cmd.roundId,
      cmd.peers.toList,
      "BroadcastConsensusDataProposal"
    ).void

  def broadcastBlockUnion(cmd: BroadcastUnionBlockProposal): F[Unit] =
    sendToAll(
      apiClient.consensus.addUnionBlock(cmd.proposal).run,
      cmd.roundId,
      cmd.peers.toList,
      "BroadcastUnionBlockProposal"
    ).void

  def broadcastSelectedUnionBlock(cmd: BroadcastSelectedUnionBlock): F[Unit] =
    sendToAll(
      apiClient.consensus.addSelectedUnionBlock(cmd.cb).run,
      cmd.roundId,
      cmd.peers.toList,
      "BroadcastSelectedUnionBlock"
    ).void

  def sendToAll(
    f: PeerClientMetadata => F[Boolean],
    roundId: RoundId,
    peers: List[PeerData],
    msg: String
  ): F[List[Boolean]] =
    peers.traverse(
      pd =>
        f(pd.peerMetadata.toPeerClientMetadata).onError {
          case _: SocketTimeoutException =>
            observationService
              .put(Observation.create(pd.peerMetadata.id, RequestTimeoutOnConsensus(roundId))(keyPair))
              .void
        }.flatTap(_ => logger.debug(s"Consensus ${roundId} sending msg ${msg}"))
          .handleErrorWith(
            e => logger.error(e)(s"Cannot send consensus round=${roundId} message ${msg}") >> false.pure[F]
          )
    )

}

case class RoundDataRemote(
  roundId: RoundId,
  peers: Set[(PeerMetadata, NonEmptyList[MajorityHeight])],
  lightPeers: Set[(PeerMetadata, NonEmptyList[MajorityHeight])],
  facilitatorId: FacilitatorId,
  transactions: List[Transaction],
  tipsSOE: TipSoe,
  messages: Seq[ChannelMessage],
  observations: List[Observation]
)

object RoundDataRemote {
  implicit val roundDataRemoteDecoder: Decoder[RoundDataRemote] = deriveDecoder
  implicit val roundDataRemoteEncoder: Encoder[RoundDataRemote] = deriveEncoder
}
