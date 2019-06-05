package org.constellation.p2p

import java.net.{InetSocketAddress, URI}

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.util.Timeout
import cats.data.Validated.{Invalid, Valid}
import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.CustomDirectives.IPEnforcer
import org.constellation.consensus.EdgeProcessor.logger
import org.constellation.consensus._
import org.constellation.p2p.routes.BlockBuildingRoundRoute
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.storage.transactions.TransactionStatus
import org.constellation.util._
import org.constellation.{DAO, ResourceInfo}
import org.json4s.native
import org.json4s.native.Serialization

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

case class PeerAuthSignRequest(salt: Long)

case class PeerRegistrationRequest(host: String, port: Int, id: Id, resourceInfo: ResourceInfo)

case class PeerUnregister(host: String, port: Int, id: Id)

object PeerAPI {

  case class EdgeResponse(
    soe: Option[SignedObservationEdgeCache] = None,
    cb: Option[CheckpointCache] = None
  )

}

class PeerAPI(override val ipManager: IPManager, nodeActor: ActorRef)(implicit system: ActorSystem,
                                                                      val timeout: Timeout,
                                                                      val dao: DAO)
    extends Json4sSupport
    with CommonEndpoints
    with IPEnforcer
    with StrictLogging
    with MetricTimerDirective {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val executionContext: ExecutionContext =
    dao.edgeExecutionContext // system.dispatchers.lookup("peer-api-dispatcher")

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  private val config: Config = ConfigFactory.load()
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
  private[p2p] val postEndpoints =
    post {
      pathPrefix("channel") {
        path("neighborhood") {
          entity(as[Id]) { peerId =>
            val distanceSorted = dao.channelService.toMap().unsafeRunSync().toSeq.sortBy {
              case (channelId, meta) =>
                Distance.calculate(meta.channelId, peerId)
            } // TODO: Determine appropriate fraction to respond with.
            complete(Seq(distanceSorted.head._2))
          }
        }
      } ~
        path("faucet") {
          entity(as[SendToAddress]) { sendRequest =>
            // TODO: Add limiting
            if (sendRequest.amountActual < (dao.processingConfig.maxFaucetSize * Schema.NormalizationFactor) &&
                dao.addressService
                  .lookup(dao.selfAddressStr)
                  .unsafeRunSync()
                  .map { _.balance }
                  .getOrElse(0L) > (dao.processingConfig.maxFaucetSize * Schema.NormalizationFactor * 5)) {

              val tx = createTransaction(dao.selfAddressStr,
                                         sendRequest.dst,
                                         sendRequest.amountActual,
                                         dao.keyPair,
                                         normalized = false)
              logger.info(s"faucet create transaction with hash: ${tx.hash} send to address $sendRequest")

              dao.transactionService.put(TransactionCacheData(tx)).unsafeRunSync()

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
                optionalHeaderValueByName("ReplyTo") { replyToOpt =>
                  val maybeData = getHostAndPortFromRemoteAddress(ip)
                  val knownHost = maybeData.exists(
                    i => dao.peerInfo.unsafeRunSync().exists(_._2.client.hostName == i.canonicalHostName)
                  )
                  dao.metrics.incrementMetric("peerApiRXFinishedCheckpoint")
                  EdgeProcessor.handleFinishedCheckpoint(fc).map { result =>
                    replyToOpt
                      .map(URI.create)
                      .map { u =>
                        makeCallback(u, FinishedCheckpointResponse(result.isSuccess))
                      }
                  }
                  // TODO: wkoszycki should we allow requests from strangers?
                  complete(FinishedCheckpointAck(!knownHost))
                }
              }
            }
          } ~
            path("reply") {
              entity(as[FinishedCheckpointResponse]) { fc =>
                if(!fc.isSuccess){
                  dao.metrics.incrementMetric(
                    "formCheckpointSignatureResponseError"
                  )
                  logger.warn("Failure gathering signature")
                }
                complete(StatusCodes.OK)
              }
            }
        }
    }

  private[p2p] def makeCallback(u: URI, entity: AnyRef) = {
    APIClient(u.getHost, u.getPort)
      .postNonBlockingUnit(u.getPath, entity)
  }

  private val blockBuildingRoundRoute =
    createRoute(BlockBuildingRoundRoute.pathPrefix)(
      () => new BlockBuildingRoundRoute(nodeActor).createBlockBuildingRoundRoutes()
    )
  private val mixedEndpoints = {
    path("transaction") {
      put {
        entity(as[Transaction]) { tx =>
          dao.metrics.incrementMetric("transactionRXByPeerAPI")

          onComplete {
            dao.transactionService.contains(tx.hash)
              .flatMap {
                case false => dao.transactionService.put(TransactionCacheData(tx), as = TransactionStatus.Unknown)
                case _ => IO.unit
              }
              // TODO: Respond with initial tx validation
              .map(_ => StatusCodes.OK)
              .unsafeToFuture()
          } {
            case Success(statusCode) => complete(statusCode)
            case Failure(_) => complete(StatusCodes.InternalServerError)
          }
        }
      }
    }
  }
  private var pendingRegistrations = Map[String, PeerRegistrationRequest]()

  def routes(address: InetSocketAddress): Route = withTimer("peer-api") {
    val id = ipLookup(address)
    // TODO: pass id down and use it if needed

    decodeRequest {
      encodeResponse {
        // rejectBannedIP {
        signEndpoints ~ commonEndpoints ~ enforceKnownIP(address) {
          getEndpoints(address) ~ postEndpoints ~ mixedEndpoints ~ blockBuildingRoundRoute
        }
      }
    }
  }

  private def getEndpoints(address: InetSocketAddress) = {
    get {
      path("ip") {
        complete(address)
      }
    }
  }

  private def getHostAndPortFromRemoteAddress(clientIP: RemoteAddress) = {
    clientIP.toOption.map { z =>
      PeerIPData(z.getHostAddress, Some(clientIP.getPort()))
    }
  }

  private def createRoute(path: String)(routeFactory: () => Route): Route = {
    pathPrefix(path) {
      handleExceptions(exceptionHandler) {
        routeFactory()
      }
    }
  }

  private def ipLookup(address: InetSocketAddress): Option[Schema.Id] = {
    val ip = address.getAddress.getHostAddress

    def sameHost(p: PeerData) = p.peerMetadata.host == ip

    dao.peerInfo.unsafeRunSync().find(p => sameHost(p._2)).map(_._1)
  }

  def exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case e: Exception =>
        extractUri { uri =>
          logger.error(s"Request to $uri could not be handled normally", e)
          complete(HttpResponse(StatusCodes.InternalServerError))
        }
    }

}
