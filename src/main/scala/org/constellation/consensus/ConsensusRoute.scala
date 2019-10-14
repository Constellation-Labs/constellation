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
      addTransactionsProposal(ctx) ~
      addUnionBlock(ctx) ~
      addSelectedUnionBlock(ctx)
  }

  protected def participateInNewRound(ctx: RequestContext): Route =
    post {
      path(ConsensusRoute.newRoundPath) {
        entity(as[RoundDataRemote]) { cmd =>
          // proposed minHeight
          // getNextHeight (snapshotHeight + 2)
          // if (last != 2) if (last > min)

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
              complete(StatusCodes.custom(400, err.getMessage))
            case Failure(e) =>
              complete(StatusCodes.InternalServerError, e.getMessage)
            case Success(res) =>
              (IO.contextShift(ConstellationExecutionContext.bounded).shift >> consensusManager
                .continueRoundParticipation(res._1, res._2)).unsafeRunAsyncAndForget()
              complete(StatusCodes.Created)
          }
        }
      }
    }

  protected def addTransactionsProposal(ctx: RequestContext): Route =
    post {
      path(ConsensusRoute.proposalPath) {
        entity(as[LightTransactionsProposal]) { proposal =>
          logger.debug(s"LightTransactionsProposal adding proposal for round ${proposal.roundId} ")
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

  private def handleProposal(proposal: ConsensusProposal): Route =
    APIDirective.handle(consensusManager.getRound(proposal.roundId)) {
      case None =>
        (IO.contextShift(ConstellationExecutionContext.bounded).shift >> consensusManager
          .addMissed(proposal.roundId, proposal))
          .unsafeRunAsyncAndForget()
        complete(StatusCodes.Accepted)
      case Some(consensus) =>
        val add = proposal match {
          case proposal: LightTransactionsProposal => consensus.addTransactionProposal(proposal)
          case proposal: UnionBlockProposal        => consensus.addBlockProposal(proposal)
          case proposal: SelectedUnionBlock        => consensus.addSelectedBlockProposal(proposal)
        }
        (IO.contextShift(ConstellationExecutionContext.bounded).shift >> add).unsafeRunAsyncAndForget()
        complete(StatusCodes.Created)
    }

}
