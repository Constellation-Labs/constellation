package org.constellation.infrastructure.endpoints

import cats.effect.{Concurrent, ContextShift}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.constellation.checkpoint.{CheckpointAcceptanceService, CheckpointBlockValidator}
import org.constellation.consensus.Consensus.{
  ConsensusDataProposal,
  ConsensusProposal,
  RoundData,
  SelectedUnionBlock,
  UnionBlockProposal
}
import org.constellation.consensus.ConsensusManager.SnapshotHeightAboveTip
import org.constellation.consensus.{ConsensusManager, RoundDataRemote}
import org.constellation.domain.transaction.TransactionService
import org.constellation.p2p.PeerData
import org.constellation.storage.SnapshotService
import org.http4s._
import org.http4s.circe._
import RoundDataRemote._
import ConsensusDataProposal._
import UnionBlockProposal._
import SelectedUnionBlock._
import org.constellation.schema.observation.ObservationEvent

class ConsensusEndpoints[F[_]](implicit F: Concurrent[F], C: ContextShift[F]) extends Http4sDsl[F] {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  private def convert(r: RoundDataRemote): RoundData =
    RoundData(
      r.roundId,
      r.peers.map { case (p, m)      => PeerData(p, m) },
      r.lightPeers.map { case (p, m) => PeerData(p, m) },
      r.facilitatorId,
      r.transactions,
      r.tipsSOE,
      r.messages,
      r.observations
    )

  private def handleProposal(
    proposal: ConsensusProposal,
    consensusManager: ConsensusManager[F],
    checkpointBlockValidator: CheckpointBlockValidator[F]
  ): F[Response[F]] =
    consensusManager.getRound(proposal.roundId).flatMap {
      case None =>
        F.start(consensusManager.addMissed(proposal.roundId, proposal)) >> Accepted()
      case Some(consensus) =>
        proposal match {
          case proposal: ConsensusDataProposal =>
            CheckpointAcceptanceService
              .areTransactionsAllowedForAcceptance[F](proposal.transactions.toList)(
                checkpointBlockValidator
              )
              .ifM(F.start(consensus.addConsensusDataProposal(proposal)) >> Accepted(), BadRequest())
          case proposal: UnionBlockProposal =>
            F.start(consensus.addBlockProposal(proposal)) >> Accepted()
          case proposal: SelectedUnionBlock =>
            F.start(consensus.addSelectedBlockProposal(proposal)) >> Accepted()
        }
    }

  private def participateInNewRoundEndpoint(
    consensusManager: ConsensusManager[F],
    snapshotService: SnapshotService[F]
  ): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "block-round" / "new-round" =>
      for {
        cmd <- req.decodeJson[RoundDataRemote]
        participate = cmd.tipsSOE.minHeight
          .fold(F.unit) { min =>
            snapshotService.getNextHeightInterval
              .map(
                last => if (last != 2 && last > min) throw SnapshotHeightAboveTip(cmd.roundId, last, min)
              )
          }
          .flatMap(_ => consensusManager.participateInBlockCreationRound(convert(cmd)))
        response <- participate.flatMap { res =>
          F.start(C.shift >> consensusManager.continueRoundParticipation(res._1, res._2)) >> Ok()
        }.handleErrorWith {
          case err @ SnapshotHeightAboveTip(_, _, _) =>
            logger
              .error(s"Error when participating in new round: ${cmd.roundId} cause: ${err.getMessage}") >>
              BadRequest(err.getMessage)
          case err =>
            logger
              .error(s"Error when participating in new round: ${cmd.roundId} cause: ${err.getMessage}") >>
              InternalServerError()
        }
      } yield response
  }

  private def addConsensusDataProposalEndpoint(
    consensusManager: ConsensusManager[F],
    checkpointBlockValidator: CheckpointBlockValidator[F]
  ): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "block-round" / "proposal" =>
      for {
        proposal <- req.decodeJson[ConsensusDataProposal]
        _ <- logger.debug(s"ConsensusDataProposal adding proposal for round ${proposal.roundId}")
        response <- handleProposal(proposal, consensusManager, checkpointBlockValidator)
      } yield response
  }

  private def addUnionBlockEndpoint(
    consensusManager: ConsensusManager[F],
    checkpointBlockValidator: CheckpointBlockValidator[F]
  ): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "block-round" / "union" =>
      for {
        proposal <- req.decodeJson[UnionBlockProposal]
        _ <- logger.debug(s"UnionBlockProposal adding proposal for round ${proposal.roundId}")
        response <- handleProposal(proposal, consensusManager, checkpointBlockValidator)
      } yield response
  }

  private def addSelectedUnionBlockEndpoint(
    consensusManager: ConsensusManager[F],
    checkpointBlockValidator: CheckpointBlockValidator[F]
  ): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "block-round" / "selected" =>
      for {
        proposal <- req.decodeJson[SelectedUnionBlock]
        _ <- logger.debug(s"SelectedUnionBlock adding proposal for round ${proposal.roundId}")
        response <- handleProposal(proposal, consensusManager, checkpointBlockValidator)
      } yield response
  }

  def peerEndpoints(
    consensusManager: ConsensusManager[F],
    snapshotService: SnapshotService[F],
    checkpointBlockValidator: CheckpointBlockValidator[F]
  ) =
    participateInNewRoundEndpoint(consensusManager, snapshotService) <+>
      addConsensusDataProposalEndpoint(consensusManager, checkpointBlockValidator) <+>
      addUnionBlockEndpoint(consensusManager, checkpointBlockValidator) <+>
      addSelectedUnionBlockEndpoint(consensusManager, checkpointBlockValidator)

}

object ConsensusEndpoints {

  def peerEndpoints[F[_]: Concurrent: ContextShift](
    consensusManager: ConsensusManager[F],
    snapshotService: SnapshotService[F],
    checkpointBlockValidator: CheckpointBlockValidator[F]
  ): HttpRoutes[F] =
    new ConsensusEndpoints[F]().peerEndpoints(consensusManager, snapshotService, checkpointBlockValidator)
}
