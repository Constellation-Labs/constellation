package org.constellation.p2p

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.{Executors, ScheduledThreadPoolExecutor}

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, IncrementMetric, TransactionValidation}
import org.constellation.util.{CommonEndpoints, HashSignature, ServeUI}
import org.json4s.native
import org.json4s.native.Serialization
import akka.pattern.ask
import org.constellation.Data
import org.constellation.LevelDB.DBPut
import org.constellation.LevelDB.{DBGet, DBPut}
import org.constellation.consensus.Consensus.{ConsensusProposal, ConsensusVote, StartConsensusRound}
import org.constellation.consensus.EdgeProcessor.HandleTransaction
import org.constellation.consensus.{Consensus, EdgeProcessor}
import org.constellation.serializer.KryoSerializer
import org.constellation.consensus.{EdgeProcessor, Validation}
import org.constellation.p2p.PeerAPI.EdgeResponse

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

case class PeerAuthSignRequest(salt: Long = Random.nextLong())

object PeerAPI {

  case class EdgeResponse(
                         soe: Option[SignedObservationEdgeCache] = None,
                         cb: Option[CheckpointCacheData] = None
                         )

}

class PeerAPI(val dao: Data)(implicit system: ActorSystem, val timeout: Timeout)
  extends Json4sSupport with CommonEndpoints with ServeUI {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("api-dispatcher")

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  val logger = Logger(s"PeerAPI")

  val config: Config = ConfigFactory.load()

  private val getEndpoints = {
    extractClientIP { clientIP =>
      get {
        path("ip") {
          complete(clientIP.toIP.map{z => PeerIPData(z.ip.getCanonicalHostName, z.port)})
        } ~
        path("edge" / Segment) { soeHash =>
          val result = Try{
            (dao.dbActor ? DBGet(soeHash)).mapTo[Option[SignedObservationEdgeCache]].get(t=5)
          }.toOption

          val resWithCBOpt = result.map{
            cacheOpt =>
              val cbOpt = cacheOpt.flatMap{ c =>
                (dao.dbActor ? DBGet(c.signedObservationEdge.baseHash)).mapTo[Option[CheckpointCacheData]].get(t=5)
                  .filter{_.checkpointBlock.checkpoint.edge.signedObservationEdge == c.signedObservationEdge}
              }
              EdgeResponse(cacheOpt, cbOpt)
          }

          complete(resWithCBOpt.getOrElse(EdgeResponse()))
        }
      }
    }
  }

  private val postEndpoints =
    post {
      path ("sign") {
        entity(as[PeerAuthSignRequest]) { e =>
          complete(hashSign(e.salt.toString, dao.keyPair))
        }
      } ~
      path("checkpointEdgeVote") {
        entity(as[Array[Byte]]) { e =>
            val message = KryoSerializer.deserialize(e).asInstanceOf[ConsensusVote[Consensus.Checkpoint]]

            dao.consensus ! message

          complete(StatusCodes.OK)
        }
      } ~
      path("checkpointEdgeProposal") {
        entity(as[Array[Byte]]) { e =>
            val message = KryoSerializer.deserialize(e).asInstanceOf[ConsensusProposal[Consensus.Checkpoint]]

            dao.consensus ! message

          complete(StatusCodes.OK)
        }
      }
    }

  private val mixedEndpoints = {
    path("transaction" / Segment) { s =>
      put {
        entity(as[Transaction]) {
          tx =>
            Future {
              /*
                Future {
              dao.metricsManager ! IncrementMetric("transactionMessagesReceived")
              EdgeProcessor.handleTransaction(tx, dao)(dao.edgeExecutionContext)
            }(dao.edgeExecutionContext)
               */
          //    logger.debug(s"transaction endpoint, $tx")
              dao.edgeProcessor ! HandleTransaction(tx)
            }

            complete(StatusCodes.OK)
        }
      } ~
      get {
        val memPoolPresence = dao.transactionMemPool.exists(_.hash == s)
        val response = if (memPoolPresence) {
          TransactionQueryResponse(s, dao.transactionMemPool.collectFirst{case x if x.hash == s => x}, inMemPool = true, inDAG = false, None)
        } else {
          (dao.dbActor ? DBGet(s)).mapTo[Option[TransactionCacheData]].get().map{
            cd =>
              TransactionQueryResponse(s, Some(cd.transaction), memPoolPresence, cd.inDAG, cd.cbBaseHash)
          }.getOrElse{
            TransactionQueryResponse(s, None, inMemPool = false, inDAG = false, None)
          }
        }

        complete(response)
      } ~ complete (StatusCodes.BadRequest)
    } ~
    path("checkpoint" / Segment) { s =>
      put {
        entity(as[CheckpointBlock]) {
          cb =>
            Future{
              EdgeProcessor.handleCheckpoint(cb, dao)(dao.edgeExecutionContext)
            }(dao.edgeExecutionContext)
            complete(StatusCodes.OK)
        }
      } ~
      get {
        complete("CB goes here")
      } ~ complete (StatusCodes.BadRequest)
    }
  }

  val routes: Route = {
    getEndpoints ~ postEndpoints ~ mixedEndpoints ~ commonEndpoints // ~
    //  faviconRoute ~ jsRequest ~ serveMainPage // <-- Temporary for debugging, control routes disabled.
  }

}
