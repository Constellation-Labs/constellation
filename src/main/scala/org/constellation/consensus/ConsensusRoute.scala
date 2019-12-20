package org.constellation.consensus

import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{extractRequestContext, path, _}
import akka.http.scaladsl.server.{RequestContext, Route}
import cats.effect.IO
import constellation._
import cats.implicits._
import com.softwaremill.sttp.SttpBackend
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.consensus.Consensus._
import org.constellation.consensus.ConsensusManager.SnapshotHeightAboveTip
import org.constellation.p2p.PeerData
import org.constellation.storage.SnapshotService
import org.constellation.util.{APIClient, APIDirective}
import org.constellation.ConstellationExecutionContext
import org.constellation.checkpoint.CheckpointAcceptanceService
import org.constellation.domain.transaction.TransactionService
import org.json4s.native
import org.json4s.native.Serialization
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ConsensusRoute {
  val pathPrefix = "block-round"
  val proposalPath = "proposal"
  val proposalFullPath = s"$pathPrefix/$proposalPath"
  val unionPath = "union"
  val unionFullPath = s"$pathPrefix/$unionPath"
  val newRoundPath = "new-round"
  val newRoundFullPath = s"$pathPrefix/$newRoundPath"
  val selectedPath = "selected"
  val selectedFullPath = s"$pathPrefix/$selectedPath"
}

class ConsensusRoute(
  consensusManager: ConsensusManager[IO],
  snapshotService: SnapshotService[IO],
  transactionService: TransactionService[IO],
  backend: SttpBackend[Future, Nothing]
) extends Json4sSupport {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val serialization: Serialization.type = native.Serialization

  def convert(r: RoundDataRemote): RoundData =
    RoundData(
      r.roundId,
      r.peers.map(p => PeerData(p, APIClient.apply(p.host, p.httpPort)(backend))),
      r.lightPeers.map(p => PeerData(p, APIClient.apply(p.host, p.httpPort)(backend))),
      r.facilitatorId,
      r.transactions,
      r.tipsSOE,
      r.messages,
      r.observations
    )

  def createBlockBuildingRoundRoutes(): Route = extractRequestContext { ctx =>
    participateInNewRound(ctx) ~
      addConsensusDataProposal(ctx) ~
      addUnionBlock(ctx) ~
      addSelectedUnionBlock(ctx)
  }

  protected def participateInNewRound(ctx: RequestContext): Route =
    post {
      path(ConsensusRoute.newRoundPath) {
        entity(as[RoundDataRemote]) { cmd =>
          val participate = cmd.tipsSOE.minHeight
            .fold(IO.unit) { min =>
              snapshotService.getNextHeightInterval
                .map(
                  last => if (last != 2 && last > min) throw SnapshotHeightAboveTip(cmd.roundId, last, min)
                )
            }
            .flatMap(_ => consensusManager.participateInBlockCreationRound(convert(cmd)))

          APIDirective.onHandle(participate) {
            case Failure(err: SnapshotHeightAboveTip) =>
              logger.error(s"Error when participating in new round: ${cmd.roundId} cause: ${err.getMessage}", err)
              complete(StatusCodes.custom(400, err.getMessage))
            case Failure(e) =>
              logger.error(s"Error when participating in new round: ${cmd.roundId} cause: ${e.getMessage}", e)
              complete(StatusCodes.InternalServerError, e.getMessage)
            case Success(res) =>
              (IO.contextShift(ConstellationExecutionContext.bounded).shift >> consensusManager
                .continueRoundParticipation(res._1, res._2)).unsafeRunAsyncAndForget()
              complete(StatusCodes.Created)
          }
        }
      }
    }

  protected def addConsensusDataProposal(ctx: RequestContext): Route =
    post {
      path(ConsensusRoute.proposalPath) {
        entity(as[ConsensusDataProposal]) { proposal =>
          logger.debug(s"ConsensusDataProposal adding proposal for round ${proposal.roundId} ")
          handleProposal(proposal)
        }
      }
    }

  protected def addUnionBlock(ctx: RequestContext): Route =
    post {
      path(ConsensusRoute.unionPath) {
        entity(as[UnionBlockProposal]) { proposal =>
          logger.debug(s"UnionBlockProposal adding proposal for round ${proposal.roundId} ")
          handleProposal(proposal)
        }
      }
    }

  protected def addSelectedUnionBlock(ctx: RequestContext): Route =
    post {
      path(ConsensusRoute.selectedPath) {
        entity(as[SelectedUnionBlock]) { proposal =>
          logger.debug(s"SelectedUnionBlock adding proposal for round ${proposal.roundId} ")
          handleProposal(proposal)
        }
      }
    }

  private def handleProposal(proposal: ConsensusProposal): Route = {
    implicit val cs = IO.contextShift(ConstellationExecutionContext.bounded)

    APIDirective.handle(consensusManager.getRound(proposal.roundId).flatMap {
      _ match {
        case None =>
          (consensusManager
            .addMissed(proposal.roundId, proposal))
            .start >> StatusCodes.Accepted.pure[IO]
        case Some(consensus) =>
          proposal match {
            case proposal: ConsensusDataProposal =>
              CheckpointAcceptanceService
                .areTransactionsAllowedForAcceptance[IO](proposal.transactions.toList)(
                  transactionService.transactionChainService
                )
                .flatMap { allowed =>
                  if (allowed) {
                    consensus.addConsensusDataProposal(proposal).start >> StatusCodes.Accepted.pure[IO]
                  } else {
                    StatusCodes.BadRequest.pure[IO]
                  }
                }
            case proposal: UnionBlockProposal =>
              consensus.addBlockProposal(proposal).start >> StatusCodes.Accepted.pure[IO]
            case proposal: SelectedUnionBlock =>
              consensus.addSelectedBlockProposal(proposal).start >> StatusCodes.Accepted.pure[IO]
          }
      }
    })(complete(_))
  }

}
