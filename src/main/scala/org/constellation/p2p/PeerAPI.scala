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
import org.constellation.{DAO, HostPort}
import org.constellation.consensus.Consensus.{ConsensusProposal, ConsensusVote}
import org.constellation.consensus.EdgeProcessor.{FinishedCheckpoint, FinishedCheckpointResponse, HandleCheckpoint, SignatureRequest, handleTransaction}
import org.constellation.consensus.{Consensus, EdgeProcessor}
import org.constellation.p2p.PeerAPI.EdgeResponse
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.{CommonEndpoints, EncodedPublicKey, SingleHashSignature}
import org.json4s.native
import org.json4s.native.Serialization

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

case class PeerAuthSignRequest(salt: Long)
case class PeerRegistrationRequest(host: String, port: Int, key: String) {
  def id = Id(EncodedPublicKey(key)) // TODO: Just send full Id class
}
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
    clientIP.toOption.map{z => PeerIPData(z.getHostAddress, Some(clientIP.getPort()))}
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
          val hash = e.salt.toString

          val signature = hashSign(hash, dao.keyPair)
          complete(SingleHashSignature(hash, signature))
        }
      } ~
      path("register") {
        extractClientIP { clientIP =>
          entity(as[PeerRegistrationRequest]) { request =>
            val hostAddress = clientIP.toOption.map{_.getHostAddress}
            logger.info(s"Received peer registration request $request on $clientIP $hostAddress ${clientIP.toOption} ${clientIP.toIP}")
            val maybeData = getHostAndPortFromRemoteAddress(clientIP)
            maybeData match {
              case Some(PeerIPData(host, _)) =>
                logger.info("Parsed host and port, sending peer manager request")
                dao.peerManager ! PendingRegistration(host, request)
                pendingRegistrations = pendingRegistrations.updated(host, request)
                complete(StatusCodes.OK)
              case None =>
                logger.info(s"Failed to parse host and port for $request")
                complete(StatusCodes.BadRequest)
            }
          }
        }
      }
    } ~
      get {
        pathPrefix("registration") {
          path("request") {
            complete(dao.peerRegistrationRequest) // Include status also
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
          extractClientIP { ip =>
/*
            ip.toOption.foreach{inet =>
              val hp = HostPort(inet.getHostAddress, 9001) // TODO: Change this to non-hardcoded port and send a response telling other node to re-register
              PeerManager.attemptRegisterPeer(hp)
            }
*/

            entity(as[SignatureRequest]) { sr =>
              dao.metricsManager ! IncrementMetric("peerApiRXSignatureRequest")
              onComplete(
                futureTryWithTimeoutMetric(
                  EdgeProcessor.handleSignatureRequest(sr), "peerAPIHandleSignatureRequest"
                )(dao.signatureResponsePool, dao)
              ) {
                result => // ^ Errors captured above
                  val knownHost = dao.peerInfo.exists(_._2.client.hostName == ip)
                  val maybeResponse = result.toOption.flatMap {
                    _.toOption
                  }.map{_.copy(reRegister = !knownHost)}
                  complete(maybeResponse)
              }
            }
          }
        }
      } ~
        pathPrefix("finished") {
        path ("checkpoint") {

          extractClientIP { ip =>
/*
            ip.toOption.foreach { inet =>
              val hp = HostPort(inet.getHostAddress, 9001) // TODO: Change this to non-hardcoded port and send a response telling other node to re-register
              PeerManager.attemptRegisterPeer(hp)
            }
*/

            entity(as[FinishedCheckpoint]) { fc =>
              // TODO: Validation / etc.
              dao.metricsManager ! IncrementMetric("peerApiRXFinishedCheckpoint")
              onComplete(
                futureTryWithTimeoutMetric(
                  if (fc.checkpointBlock.simpleValidation()) {
                    dao.threadSafeTipService.accept(fc.checkpointBlock)
                  } else Future.successful(), "peerAPIFinishedCheckpointRX"
                )(dao.edgeExecutionContext, dao)
              ) {
                result => // ^ Errors captured above
                  val maybeResponse = result.toOption.flatMap {
                    _.toOption
                  }.map{
                    _ =>
                      val knownHost = dao.peerInfo.exists(_._2.client.hostName == ip)
                      FinishedCheckpointResponse(!knownHost)
                  }
                  complete(maybeResponse)
              }
            }
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
      }
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
   // rejectBannedIP {
      signEndpoints ~ commonEndpoints ~  // { //enforceKnownIP
        getEndpoints ~ postEndpoints ~ mixedEndpoints
    //  }
   // } // ~
    //  faviconRoute ~ jsRequest ~ serveMainPage // <-- Temporary for debugging, control routes disabled.

  }

}
/*      get {
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
    } ~*/
