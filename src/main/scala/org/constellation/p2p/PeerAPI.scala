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
import org.constellation.CustomDirectives.IPEnforcer
import org.constellation.DAO
import org.constellation.consensus.Consensus.{ConsensusProposal, ConsensusVote}
import org.constellation.consensus.EdgeProcessor.{FinishedCheckpoint, HandleCheckpoint, SignatureRequest, handleTransaction}
import org.constellation.consensus.{Consensus, EdgeProcessor}
import org.constellation.p2p.PeerAPI.EdgeResponse
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.CommonEndpoints
import org.json4s.native
import org.json4s.native.Serialization

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

case class PeerAuthSignRequest(salt: Long = Random.nextLong())
case class PeerRegistrationRequest(host: String, port: Int, key: String)
case class PeerUnregister(host: String, port: Int, key: String)


object PeerAPI {

  case class EdgeResponse(
                         soe: Option[SignedObservationEdgeCache] = None,
                         cb: Option[CheckpointCacheData] = None
                         )

}

class PeerAPI(override val ipManager: IPManager)(implicit system: ActorSystem, val timeout: Timeout, val dao: DAO)
  extends Json4sSupport with CommonEndpoints with IPEnforcer {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("peer-api-dispatcher")

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  private val logger = Logger(s"PeerAPI")

  private val config: Config = ConfigFactory.load()

  private var pendingRegistrations = Map[String, PeerRegistrationRequest]()

  private def getHostAndPortFromRemoteAddress(clientIP: RemoteAddress) = {
    clientIP.toIP.map{z => PeerIPData(z.ip.getCanonicalHostName, z.port)}
  }

  private val getEndpoints = {
    get {
      extractClientIP { clientIP =>
        path("ip") {
          complete(clientIP.toIP.map{z => PeerIPData(z.ip.getCanonicalHostName, z.port)})
        } ~
        path("edge" / Segment) { soeHash =>
          val cacheOpt = dao.dbActor.getSignedObservationEdgeCache(soeHash)

          val cbOpt = cacheOpt.flatMap { c =>
            dao.dbActor.getCheckpointCacheData(c.signedObservationEdge.baseHash)
              .filter{_.checkpointBlock.checkpoint.edge.signedObservationEdge == c.signedObservationEdge}
          }

          val resWithCBOpt = EdgeResponse(cacheOpt, cbOpt)

          complete(resWithCBOpt)
        }
      }
    }
  }

  private val signEndpoints =
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
      path("register") {
        extractClientIP { clientIP =>
          entity(as[PeerRegistrationRequest]) { request =>
            val maybeData = getHostAndPortFromRemoteAddress(clientIP)
            maybeData match {
              case Some(PeerIPData(host, portOption)) =>
                dao.peerManager ! PendingRegistration(host, request)
                pendingRegistrations = pendingRegistrations.updated(host, request)
                complete(StatusCodes.OK)
              case None =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      }
    }

  private val postEndpoints =
    post {
      path ("deregister") {
        extractClientIP { clientIP =>
          entity(as[PeerUnregister]) { request =>
            val maybeData = getHostAndPortFromRemoteAddress(clientIP)
            maybeData match {
              case Some(PeerIPData(host, portOption)) =>
                dao.peerManager ! Deregistration(request.host, request.port, request.key)
                complete(StatusCodes.OK)
              case None =>
                complete(StatusCodes.BadRequest)
            }
          }
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
            dao.metricsManager ! IncrementMetric("apiRXSignatureRequest")
            onComplete(Future{EdgeProcessor.handleSignatureRequest(sr)}(dao.signatureResponsePool)) {
              case Failure(e) =>
                e.printStackTrace()
                dao.metricsManager ! IncrementMetric("signatureResponseFAILEDRespondingwithEmpty")
                complete(None)
              case Success(r) =>
                complete(Some(r))
            }
          }
        }
      } ~
        pathPrefix("finished") {
        path ("checkpoint") {
          entity(as[FinishedCheckpoint]) { fc =>
            // TODO: Validation / etc.
            dao.metricsManager ! IncrementMetric("apiRXFinishedCheckpoint")
            Future{dao.threadSafeTipService.accept(fc.checkpointBlock)}(dao.edgeExecutionContext)
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
            dao.metricsManager ! IncrementMetric("transactionRXByAPI")
              // TDOO: Change to ask later for status info
         //   dao.edgeProcessor ! HandleTransaction(tx)

            Future {
              if (dao.nodeState == NodeState.Ready) {
                handleTransaction(tx)
              }
            }(dao.edgeExecutionContext)

            complete(StatusCodes.OK)
        }
      } ~
      get {
        val memPoolPresence = dao.transactionMemPool.exists(_.hash == s)
        val response = if (memPoolPresence) {
          TransactionQueryResponse(s, dao.transactionMemPool.collectFirst{case x if x.hash == s => x}, inMemPool = true, inDAG = false, None)
        } else {
          dao.dbActor.getTransactionCacheData(s).map {
            cd =>
              TransactionQueryResponse(s, Some(cd.transaction), memPoolPresence, cd.inDAG, cd.cbBaseHash)
          }.getOrElse{
            TransactionQueryResponse(s, None, inMemPool = false, inDAG = false, None)
          }
        }

        complete(response)
      } ~ complete (StatusCodes.BadRequest)
    } ~
    path("checkpoint" / Segment) { s => // Deprecated
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
        val res = dao.dbActor.getCheckpointCacheData(s).map{_.checkpointBlock}
        complete(res)
      } ~ complete (StatusCodes.BadRequest)
    }
  }

  val routes: Route = {
    rejectBannedIP {
      signEndpoints ~ commonEndpoints ~  { //enforceKnownIP
        getEndpoints ~ postEndpoints ~ mixedEndpoints
      }
    } // ~
    //  faviconRoute ~ jsRequest ~ serveMainPage // <-- Temporary for debugging, control routes disabled.

  }

}
