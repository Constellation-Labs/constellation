package org.constellation.p2p

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.DAO
import org.constellation.LevelDB.DBGet
import org.constellation.consensus.Consensus
import org.constellation.consensus.Consensus.{ConsensusProposal, ConsensusVote}
import org.constellation.consensus.EdgeProcessor.{FinishedCheckpoint, HandleCheckpoint, HandleTransaction, SignatureRequest, SignatureResponse}
import org.constellation.p2p.PeerAPI.EdgeResponse
import org.constellation.primitives.Schema._
import org.constellation.primitives.SetNodeStatus
import org.constellation.serializer.KryoSerializer
import org.constellation.util.{CommonEndpoints, ServeUI}
import org.json4s.native
import org.json4s.native.Serialization

import scala.concurrent.ExecutionContext
import scala.util.{Random, Try}

case class PeerAuthSignRequest(salt: Long = Random.nextLong())

object PeerAPI {

  case class EdgeResponse(
                         soe: Option[SignedObservationEdgeCache] = None,
                         cb: Option[CheckpointCacheData] = None
                         )

}

class PeerAPI(val dao: DAO)(implicit system: ActorSystem, val timeout: Timeout)
  extends Json4sSupport with CommonEndpoints with ServeUI {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("peer-api-dispatcher")

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
      path("status") {
        entity(as[SetNodeStatus]) { sns =>
          dao.peerManager ! sns
          complete(StatusCodes.OK)
        }
      } ~
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
      } ~
      pathPrefix("request") {
        path("signature") {
          entity(as[SignatureRequest]) { sr =>
            // Temporary, need to go thru flow


            dao.edgeProcessor ! sr
            //val res = if (dao.nodeState == NodeStatus.Ready) {
              /*val blockWithSigAdded = (dao.cpSigner ? cb).mapTo[Option[CheckpointBlock]].get()
              blockWithSigAdded.foreach{
                b =>
                  Future {
                    EdgeProcessor.handleCheckpoint(b, dao)(dao.edgeExecutionContext)
                  }(dao.edgeExecutionContext)
              }
              blockWithSigAdded*/
             // None
            //} else None
            complete(StatusCodes.OK)
          }
        }
      } ~ pathPrefix("response") {
        path("signature") {
          entity(as[SignatureResponse]) { sr =>
            dao.edgeProcessor ! sr
            complete(StatusCodes.OK)
          }
        }
      } ~ pathPrefix("finished") {
        path ("checkpoint") {
          entity(as[FinishedCheckpoint]) { fc =>
            dao.edgeProcessor ! fc
            complete(StatusCodes.OK)
          }
        }
      }
    }

  private val mixedEndpoints = {
    path("transaction" / Segment) { s =>
      put {
        entity(as[Transaction]) {
          tx =>
            if (dao.nodeState == NodeState.Ready) {
              // TDOO: Change to ask later for status info
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
            val ret = if (dao.nodeState == NodeState.Ready) {
              dao.edgeProcessor ? HandleCheckpoint(cb)
            } else None
            complete(StatusCodes.OK)
        }
      } ~
      get {
        val res = (dao.dbActor ? DBGet(s)).mapTo[Option[CheckpointCacheData]].get().map{_.checkpointBlock}
        complete(res)
      } ~ complete (StatusCodes.BadRequest)
    }
  }

  val routes: Route = {
    getEndpoints ~ postEndpoints ~ mixedEndpoints ~ commonEndpoints // ~
    //  faviconRoute ~ jsRequest ~ serveMainPage // <-- Temporary for debugging, control routes disabled.
  }

}
