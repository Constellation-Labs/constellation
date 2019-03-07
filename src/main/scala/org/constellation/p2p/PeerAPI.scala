package org.constellation.p2p

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.{ExceptionHandler, RequestContext, Route}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.CustomDirectives.IPEnforcer
import org.constellation.DAO
import org.constellation.consensus.EdgeProcessor
import org.constellation.consensus.EdgeProcessor.{FinishedCheckpoint, FinishedCheckpointResponse, SignatureRequest}
import org.constellation.p2p.routes.BlockBuildingRoundRoute
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.util.{CommonEndpoints, SingleHashSignature, MetricTimerDirective}
import org.json4s.native
import org.json4s.native.Serialization

import scala.concurrent.ExecutionContext

case class PeerAuthSignRequest(salt: Long)

case class PeerRegistrationRequest(host: String, port: Int, id: Id)

case class PeerUnregister(host: String, port: Int, id: Id)

object PeerAPI {

  case class EdgeResponse(
    soe: Option[SignedObservationEdgeCache] = None,
    cb: Option[CheckpointCacheData] = None
  )

}

class PeerAPI(override val ipManager: IPManager, nodeActor: ActorRef)(
  implicit system: ActorSystem,
                                                 val timeout: Timeout,
                                                 val dao: DAO)
    extends Json4sSupport
    with CommonEndpoints
    with IPEnforcer
    with StrictLogging
    with MetricTimerDirective {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val executionContext
    : ExecutionContext = dao.edgeExecutionContext // system.dispatchers.lookup("peer-api-dispatcher")

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  private val config: Config = ConfigFactory.load()

  private var pendingRegistrations = Map[String, PeerRegistrationRequest]()

  private def getHostAndPortFromRemoteAddress(clientIP: RemoteAddress) = {
    clientIP.toOption.map { z =>
      PeerIPData(z.getHostAddress, Some(clientIP.getPort()))
    }
  }

  private val getEndpoints = {
    get {
      extractClientIP { clientIP =>
        path("ip") {
          complete(clientIP.toIP.map { z =>
            PeerIPData(z.ip.getCanonicalHostName, z.port)
          })
        } /*~
        path("edge" / Segment) { soeHash =>
          val cacheOpt = dao.dbActor.getSignedObservationEdgeCache(soeHash)

          val cbOpt = cacheOpt.flatMap { c =>
            dao.dbActor.getCheckpointCacheData(c.signedObservationEdge.baseHash)
              .filter{_.checkpointBlock.checkpoint.edge.signedObservationEdge == c.signedObservationEdge}
          }

          val resWithCBOpt = EdgeResponse(cacheOpt, cbOpt)

          complete(resWithCBOpt)
        }*/
      }
    }
  }

  def exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case e: Exception =>
        extractUri { uri =>
          logger.error(s"Request to $uri could not be handled normally", e)
          complete(HttpResponse(StatusCodes.InternalServerError))
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
        path("sign") {
          entity(as[PeerAuthSignRequest]) { e =>
            val hash = e.salt.toString

            val signature = hashSign(hash, dao.keyPair)
            complete(SingleHashSignature(hash, signature))
          }
        } ~
        path("register") {
          extractClientIP { clientIP =>
            entity(as[PeerRegistrationRequest]) { request =>
              val hostAddress = clientIP.toOption.map { _.getHostAddress }
              logger.debug(
                s"Received peer registration request $request on $clientIP $hostAddress ${clientIP.toOption} ${clientIP.toIP}"
              )
              val maybeData = getHostAndPortFromRemoteAddress(clientIP)
              maybeData match {
                case Some(PeerIPData(host, _)) =>
                  logger.debug("Parsed host and port, sending peer manager request")
                  dao.peerManager ! PendingRegistration(host, request)
                  pendingRegistrations = pendingRegistrations.updated(host, request)
                  complete(StatusCodes.OK)
                case None =>
                  logger.warn(s"Failed to parse host and port for $request")
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
      path("faucet") {
        entity(as[SendToAddress]) { sendRequest =>
          // TODO: Add limiting
          if (sendRequest.amountActual < (dao.processingConfig.maxFaucetSize * Schema.NormalizationFactor) &&
              dao.addressService
                .get(dao.selfAddressStr)
                .map { _.balance }
                .getOrElse(0L) > (dao.processingConfig.maxFaucetSize * Schema.NormalizationFactor * 5)) {
            logger.info(s"send transaction to address $sendRequest")

            val tx = createTransaction(dao.selfAddressStr,
                                       sendRequest.dst,
                                       sendRequest.amountActual,
                                       dao.keyPair,
                                       normalized = false)
            dao.threadSafeTXMemPool.put(tx, overrideLimit = true)
            dao.metrics.incrementMetric("faucetRequest")

            complete(Some(tx.hash))
          } else {
            logger.warn(s"Invalid faucet request $sendRequest")
            dao.metrics.incrementMetric("faucetInvalidRequest")
            complete(None)
          }
        }
      } ~
        path("deregister") {
          extractClientIP { clientIP =>
            entity(as[PeerUnregister]) { request =>
              val maybeData = getHostAndPortFromRemoteAddress(clientIP)
              maybeData match {
                case Some(PeerIPData(host, portOption)) =>
                  dao.peerManager ! Deregistration(request.host, request.port, request.id)
                  complete(StatusCodes.OK)
                case None =>
                  complete(StatusCodes.BadRequest)
              }
            }
          }
        } ~
        pathPrefix("request") {
          path("signature") {
            extractClientIP { ip =>
              entity(as[SignatureRequest]) { sr =>
                onComplete(
                  EdgeProcessor.handleSignatureRequest(sr)
                ) { result =>
                  complete(result.toOption.flatMap(_.toOption))
                }
              }
            }
          }
        } ~
        pathPrefix("finished") {
          path("checkpoint") {

            extractClientIP { ip =>
              /*
            ip.toOption.foreach { inet =>
              val hp = HostPort(inet.getHostAddress, 9001) // TODO: Change this to non-hardcoded port and send a response telling other node to re-register
              PeerManager.attemptRegisterPeer(hp)
            }
               */

              entity(as[FinishedCheckpoint]) { fc =>
                // TODO: Validation / etc.
                dao.metrics.incrementMetric("peerApiRXFinishedCheckpoint")
                onComplete(
                  EdgeProcessor.handleFinishedCheckpoint(fc)
                ) { result => // ^ Errors captured above
                  val maybeResponse = result.flatten.map { _ =>
                    val maybeData = getHostAndPortFromRemoteAddress(ip)
                    val knownHost = maybeData.exists(
                      i => dao.peerInfo.exists(_._2.client.hostName == i.canonicalHostName)
                    )
                    FinishedCheckpointResponse(!knownHost)
                  }.toOption
                  complete(maybeResponse)
                }
              }
            }
          }
        }
    }

  private val blockBuildingRoundRoute =
    createRoute(BlockBuildingRoundRoute.pathPrefix)(() => new BlockBuildingRoundRoute(nodeActor).createBlockBuildingRoundRoutes())

  private def createRoute(path: String)(routeFactory: () => Route): Route = {
    pathPrefix(path) {
      handleExceptions(exceptionHandler) {
        routeFactory()
      }
    }
  }



  private val mixedEndpoints = {
    path("transaction") {
      put {
        entity(as[Transaction]) { tx =>
          dao.metrics.incrementMetric("transactionRXByPeerAPI")
          dao.transactionService.update(tx.hash, { tcd =>
            tcd
          }, TransactionCacheData(tx))
          // TODO: Respond with initial tx validation
          complete(StatusCodes.OK)
        }
      }
    }
  }

  val routes: Route = withTimer("peer-api") {
    decodeRequest {
      encodeResponse {
        // rejectBannedIP {
        signEndpoints ~ commonEndpoints ~ blockBuildingRoundRoute ~ // { //enforceKnownIP
          getEndpoints ~ postEndpoints ~ mixedEndpoints
      }
    }
  }

}
