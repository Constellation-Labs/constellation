package org.constellation.p2p.routes
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{extractRequestContext, path, _}
import akka.http.scaladsl.server.{RequestContext, Route}
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.consensus.CrossTalkConsensus.ParticipateInBlockCreationRound
import org.constellation.consensus.Round.{LightTransactionsProposal, RoundData, UnionBlockProposal}
import org.constellation.consensus.RoundDataRemote
import org.constellation.primitives.PeerData
import org.constellation.util.APIClient
import org.json4s.native
import org.json4s.native.Serialization
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

object BlockBuildingRoundRoute {
  val pathPrefix = "block-round"
  val proposalPath = "proposal"
  val proposalFullPath = s"$pathPrefix/$proposalPath"
  val unionPath = "union"
  val unionFullPath = s"$pathPrefix/$unionPath"
  val newRoundPath = "new-round"
  val newRoundFullPath = s"$pathPrefix/$newRoundPath"

  def convert(r: RoundDataRemote)(implicit executionContext: ExecutionContext): RoundData = {
    RoundData(
      r.roundId,
      r.peers.map(p => PeerData(p, APIClient.apply(p.host, p.httpPort))),
      r.facilitatorId,
      r.transactions,
      r.tipsSOE,
      r.messages
    )
  }
}

class BlockBuildingRoundRoute(nodeActor: ActorRef)(implicit system: ActorSystem,
                                                   executionContext: ExecutionContext)
    extends Json4sSupport {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val serialization: Serialization.type = native.Serialization

  def createBlockBuildingRoundRoutes(): Route = extractRequestContext { ctx =>
    participateInNewRound(ctx) ~
      addTransactionsProposal(ctx) ~
      addUnionBlock(ctx)
  }

  protected def participateInNewRound(ctx: RequestContext): Route = {
    post {
      path(BlockBuildingRoundRoute.newRoundPath) {
        entity(as[RoundDataRemote]) { cmd =>
          nodeActor ! ParticipateInBlockCreationRound(BlockBuildingRoundRoute.convert(cmd))
          complete(StatusCodes.Created)
        }
      }
    }
  }

  protected def addTransactionsProposal(ctx: RequestContext): Route = {
    post {
      path(BlockBuildingRoundRoute.proposalPath) {
        entity(as[LightTransactionsProposal]) { proposal =>
          nodeActor ! proposal
          complete(StatusCodes.Created)
        }
      }
    }
  }

  protected def addUnionBlock(ctx: RequestContext): Route = {
    post {
      path(BlockBuildingRoundRoute.unionPath) {
        entity(as[UnionBlockProposal]) { proposal =>
          nodeActor ! proposal
          complete(StatusCodes.Created)
        }
      }
    }
  }

}
