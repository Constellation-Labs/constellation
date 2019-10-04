package org.constellation.p2p

import java.net.{InetSocketAddress, URI}

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.util.Timeout
import cats.effect.IO
import cats.implicits._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.CustomDirectives.IPEnforcer
import org.constellation.consensus.{ConsensusRoute, _}
import org.constellation.domain.schema.Id
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.storage._
import org.constellation.util._
import org.constellation.{ConstellationExecutionContext, DAO, ResourceInfo}
import org.json4s.native
import org.json4s.native.Serialization

import scala.util.Random

case class PeerAuthSignRequest(salt: Long)

case class PeerRegistrationRequest(host: String, port: Int, id: Id, resourceInfo: ResourceInfo)

case class PeerUnregister(host: String, port: Int, id: Id)

object PeerAPI {

  case class EdgeResponse(
    soe: Option[SignedObservationEdgeCache] = None,
    cb: Option[CheckpointCache] = None
  )

}

class PeerAPI(override val ipManager: IPManager[IO])(
  implicit system: ActorSystem,
  val timeout: Timeout,
  val dao: DAO
) extends Json4sSupport
    with CommonEndpoints
    with IPEnforcer
    with StrictLogging
    with SimulateTimeoutDirective {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  private val config: Config = ConfigFactory.load()
  private val signEndpoints =
    post {
      path("status") {
        entity(as[SetNodeStatus]) { sns =>
          APIDirective.handle(dao.cluster.setNodeStatus(sns.id, sns.nodeStatus))(_ => complete(StatusCodes.OK))
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
                  APIDirective.handle(dao.cluster.pendingRegistration(host, request))(_ => complete(StatusCodes.OK))
                case None =>
                  logger.warn(s"Failed to parse host and port for $request")
                  complete(StatusCodes.BadRequest)
              }
            }
          }
        } ~
        path("join") {
          entity(as[HostPort]) { hp =>
            (IO
              .contextShift(ConstellationExecutionContext.bounded)
              .shift >> dao.cluster.join(hp)).unsafeRunAsyncAndForget
            complete(StatusCodes.OK)
          }
        } ~
        path("leave") {
          (IO
            .contextShift(ConstellationExecutionContext.bounded)
            .shift >> IO(dao.node.shutdown())).unsafeRunAsyncAndForget
          complete(StatusCodes.OK)
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
      pathPrefix("snapshot") {
        path("verify") {
          entity(as[SnapshotCreated]) { s =>
            APIDirective.handle(
              dao.snapshotBroadcastService.getRecentSnapshots
            ) { result =>
              val response = result match {
                case lastSnap :: _ if lastSnap.height < s.height =>
                  SnapshotVerification(VerificationStatus.SnapshotHeightAbove)
                case list if list.contains(RecentSnapshot(s.snapshot, s.height)) =>
                  SnapshotVerification(VerificationStatus.SnapshotCorrect)
                case _ => SnapshotVerification(VerificationStatus.SnapshotInvalid)
              }
              complete(response)
            }
          }
        }
      } ~
        pathPrefix("channel") {
          path("neighborhood") {
            entity(as[Id]) { peerId =>
              val distanceSorted = dao.channelService
                .toMap()
                .map(_.toSeq.sortBy {
                  case (channelId, meta) =>
                    Distance.calculate(meta.channelId, peerId)
                }) // TODO: Determine appropriate fraction to respond with.
              APIDirective.handle(distanceSorted.map(d => Seq(d.head._2)))(complete(_))
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

              val tx = createTransaction(
                dao.selfAddressStr,
                sendRequest.dst,
                sendRequest.amountActual,
                dao.keyPair,
                normalized = false
              )
              logger.debug(s"faucet create transaction with hash: ${tx.hash} send to address $sendRequest")

              dao.transactionService.put(TransactionCacheData(tx)).unsafeRunAsync(_ => ())
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
                  APIDirective.handle(dao.cluster.deregister(request.host, request.port, request.id)) { _ =>
                    complete(StatusCodes.OK)
                  }
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
              entity(as[FinishedCheckpoint]) { fc =>
                optionalHeaderValueByName("ReplyTo") { replyToOpt =>
                  val baseHash = fc.checkpointCacheData.checkpointBlock.map(_.baseHash)
                  logger.debug(
                    s"Handle finished checkpoint for cb: ${baseHash} and replyTo: $replyToOpt"
                  )

                  dao.metrics.incrementMetric("peerApiRXFinishedCheckpoint")

                  val callback = dao.checkpointAcceptanceService.accept(fc).map { result =>
                    replyToOpt
                      .map(URI.create)
                      .map { u =>
                        logger.debug(
                          s"Making callback to: ${u.toURL} acceptance of cb: ${fc.checkpointCacheData.checkpointBlock
                            .map(_.baseHash)} performed $result"
                        )
                        makeCallback(u, FinishedCheckpointResponse(true))
                      }
                  }

                  APIDirective.handle(dao.snapshotService.getNextHeightInterval) { res =>
                    (res, fc.checkpointCacheData.height) match {
                      case (_, None) =>
                        logger.warn(s"Missing height when accepting block $baseHash")
                        complete(StatusCodes.BadRequest)
                      case (2, _) =>
                        callback.unsafeToFuture()
                        complete(StatusCodes.Accepted)
                      case (nextHeight, Some(Height(min, max))) if nextHeight > min =>
                        logger.debug(
                          s"Handle finished checkpoint for cb: ${fc.checkpointCacheData.checkpointBlock
                            .map(_.baseHash)} height condition not met next interval: ${nextHeight} received: ${fc.checkpointCacheData.height.get.min}"
                        )
                        complete(StatusCodes.Conflict)
                      case (_, _) =>
                        callback.unsafeToFuture()
                        complete(StatusCodes.Accepted)
                    }
                  }

                }
              }
            }
          } ~
            path("reply") {
              entity(as[FinishedCheckpointResponse]) { fc =>
                if (!fc.isSuccess) {
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
  private val blockBuildingRoundRoute =
    createRoute(ConsensusRoute.pathPrefix)(
      () => new ConsensusRoute(dao.consensusManager, dao.snapshotService, dao.backend).createBlockBuildingRoundRoutes()
    )
  private[p2p] val mixedEndpoints = {
    path("transaction") {
      put {
        entity(as[TransactionGossip]) { gossip =>
          logger.debug(s"Received transaction tx=${gossip.hash} with path=${gossip.path}")
          dao.metrics.incrementMetric("transactionRXByPeerAPI")

          implicit val random: Random = scala.util.Random

          /* TEMPORARY DISABLED
          val rebroadcast = for {
            tcd <- dao.transactionGossiping.observe(TransactionCacheData(gossip.tx, path = gossip.path))
            peers <- dao.transactionGossiping.selectPeers(tcd)
            peerData <- dao.peerInfo.map(_.filterKeys(peers.contains).values.toList)
            _ <- contextShift.evalOn(ConstellationExecutionContext.callbacks)(
              peerData.traverse(_.client.putAsync("transaction", TransactionGossip(tcd)))
            )
            _ <- dao.metrics.incrementMetricAsync[IO]("transactionGossipingSent")
          } yield ()

          rebroadcast.unsafeRunAsyncAndForget()
           */

          (IO.contextShift(ConstellationExecutionContext.bounded).shift >> dao.transactionGossiping.observe(
            TransactionCacheData(gossip.tx, path = gossip.path)
          )).unsafeRunAsyncAndForget()

          complete(StatusCodes.OK)
        }
      }
    }
  }

  def routes(address: InetSocketAddress): Route =
    // val id = ipLookup(address) causes circular dependencies and cluster with 6 nodes unable to start due to timeouts. Consider reopen #391
    // TODO: pass id down and use it if needed
    decodeRequest {
      encodeResponse {
        // rejectBannedIP {
        signEndpoints ~ commonEndpoints ~ batchEndpoints ~
          withSimulateTimeout(dao.simulateEndpointTimeout)(ConstellationExecutionContext.unbounded) {
            enforceKnownIP(address) {
              getEndpoints(address) ~ postEndpoints ~ mixedEndpoints ~ blockBuildingRoundRoute
            }
          }
      }
    }

  private def getEndpoints(address: InetSocketAddress) =
    get {
      path("ip") {
        complete(address)
      }
    }

  private[p2p] def makeCallback(u: URI, entity: AnyRef) =
    APIClient(u.getHost, u.getPort)(dao.backend, dao)
      .postNonBlockingUnit(u.getPath, entity)

  private def getHostAndPortFromRemoteAddress(clientIP: RemoteAddress) =
    clientIP.toOption.map { z =>
      PeerIPData(z.getHostAddress, Some(clientIP.getPort()))
    }

  private def createRoute(path: String)(routeFactory: () => Route): Route =
    pathPrefix(path) {
      handleExceptions(exceptionHandler) {
        routeFactory()
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

  private def ipLookup(address: InetSocketAddress): Option[Id] = {
    val ip = address.getAddress.getHostAddress

    def sameHost(p: PeerData) = p.peerMetadata.host == ip

    dao.peerInfo.unsafeRunSync().find(p => sameHost(p._2)).map(_._1)
  }
}
