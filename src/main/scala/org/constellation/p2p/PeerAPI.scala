package org.constellation.p2p

import java.net.{InetSocketAddress, URI}

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.util.Timeout
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.scalalogging.StrictLogging
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.CustomDirectives.IPEnforcer
import org.constellation.api.TokenAuthenticator
import org.constellation.consensus.{ConsensusRoute, _}
import org.constellation.domain.observation.{Observation, SnapshotMisalignment}
import org.constellation.domain.trust.TrustData
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.storage._
import org.constellation.util._
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO, ResourceInfo}
import org.json4s.native
import org.json4s.native.Serialization
import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.consensus.EdgeProcessor.chunkSerialize

import scala.concurrent.Future
import scala.util.Random

case class PeerAuthSignRequest(salt: Long)

case class PeerRegistrationRequest(host: String, port: Int, id: Id, resourceInfo: ResourceInfo)

case class PeerUnregister(host: String, port: Int, id: Id)

object PeerAPI {

  case class EdgeResponse(
    soe: Option[SignedObservationEdge] = None,
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
    with SimulateTimeoutDirective
    with TokenAuthenticator {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  implicit def exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case e: Exception =>
        extractUri { uri =>
          logger.error(s"Request to $uri could not be handled normally", e)
          complete(HttpResponse(StatusCodes.InternalServerError, entity = e.getMessage))
        }
    }

  val snapshotHeightRedownloadDelayInterval =
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval")

  private val authEnabled: Boolean = ConfigUtil.getAuthEnabled

  private def signEndpoints(socketAddress: InetSocketAddress) =
    post {
      path("status") {
        entity(as[SetNodeStatus]) { sns =>
          APIDirective.handle {
            if (sns.nodeStatus == NodeState.Offline) {
              dao.cluster.markOfflinePeer(sns.id)
            } else {
              dao.cluster.setNodeStatus(sns.id, sns.nodeStatus)
            }
          }(_ => complete(StatusCodes.OK))
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
          APIDirective.extractIP(socketAddress) { ip =>
            entity(as[PeerRegistrationRequest]) { request =>
              logger.debug(
                s"Received peer registration request $request on $ip"
              )
              logger.debug("Parsed host, sending peer manager request")
              APIDirective.handle(dao.cluster.pendingRegistration(ip, request))(
                _ => complete(StatusCodes.OK)
              )
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
            .shift >> dao.cluster.leave(IO.unit)).unsafeRunAsyncAndForget
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
  private[p2p] def postEndpoints(socketAddress: InetSocketAddress) =
    post {
      pathPrefix("snapshot") {
        path("info") {
          entity(as[Array[Array[Byte]]]) { curCheckpointHashes =>
          val curCheckpoints = curCheckpointHashes.flatMap(EdgeProcessor.chunkDeSerialize[Array[String]](_, "curCheckpointHashes"))
    val res = dao.snapshotService.getSnapshotInfo.flatMap { info =>
      logger.warn(s"snapshot/info nfo.acceptedCBSinceSnapshot - ${info.acceptedCBSinceSnapshot.toList}")
      logger.warn(s"snapshot/info curCheckpointHashes - ${curCheckpointHashes.toList}")
      val checkpointsToGet = info.acceptedCBSinceSnapshot.toList.diff(curCheckpoints.toList)
      logger.warn(s"snapshot/info checkpointsToGet - ${checkpointsToGet.toList}")
      logger.warn(s"snapshot/info checkpointsToGet.length - ${checkpointsToGet.length}")
      checkpointsToGet.toList.traverse { //todo .diff on info.acceptedCBSinceSnapshotCache with acceptedCBSinceSnapshotCache from querying peer
        dao.checkpointService.fullData(_)
      }.flatMap { cbs =>
        val snapshotInfo = info.copy(acceptedCBSinceSnapshotCache = cbs.flatten)
        val io = dao.snapshotService.recentSnapshotInfo.modify { _ =>
          logger.warn(s"snapshot/info recentSnapshotInfo updating")
          (Some(snapshotInfo), Array.empty[Byte])
        }
        io
      }
    }

    APIDirective.handle(res)(complete(_))
  }
        }~
          path("verify") {
            entity(as[SnapshotCreated]) { s =>
              APIDirective.handle(
                dao.snapshotBroadcastService.getRecentSnapshots
              ) { result =>
                logger.warn(s"snapshot received ${s}")
                // TODO: wkoszycki maybe move it SnapshotBroadcastService ?
                val response = result match {
                  case Nil => SnapshotVerification(dao.id, VerificationStatus.SnapshotHeightAbove, result)
                  case lastSnap :: _ if lastSnap.height < s.height =>
                    if (lastSnap.height + snapshotHeightRedownloadDelayInterval < s.height) {
                      (IO
                        .contextShift(ConstellationExecutionContext.bounded)
                        .shift >> dao.snapshotBroadcastService.verifyRecentSnapshots()).unsafeRunAsyncAndForget
                    }
                    SnapshotVerification(dao.id, VerificationStatus.SnapshotHeightAbove, result)
                  case list if list.contains(RecentSnapshot(s.hash, s.height, s.publicReputation)) =>
                    SnapshotVerification(dao.id, VerificationStatus.SnapshotCorrect, result)
                  case _ =>
                    (IO
                      .contextShift(ConstellationExecutionContext.bounded)
                      .shift >> dao.snapshotBroadcastService.verifyRecentSnapshots()).unsafeRunAsyncAndForget
                    SnapshotVerification(dao.id, VerificationStatus.SnapshotInvalid, result)
                }
                complete(response)
              }
            }
          }
        }} ~
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
            // TODO: Chain
            if (sendRequest.amountActual < (dao.processingConfig.maxFaucetSize * Schema.NormalizationFactor) &&
                dao.addressService
                  .lookup(dao.selfAddressStr)
                  .unsafeRunSync()
                  .map { _.balance }
                  .getOrElse(0L) > (dao.processingConfig.maxFaucetSize * Schema.NormalizationFactor * 10)) {

              val tx = dao.transactionService
                .createTransaction(
                  dao.selfAddressStr,
                  sendRequest.dst,
                  sendRequest.amountActual,
                  dao.keyPair,
                  normalized = false
                )
                .unsafeRunSync()
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
          entity(as[PeerUnregister]) { request =>
            APIDirective.handle(dao.cluster.deregister(request.host, request.port, request.id)) { _ =>
              complete(StatusCodes.OK)
            }
          }
        } ~
        pathPrefix("request") {
          path("signature") {
            APIDirective.extractIP(socketAddress) { ip =>
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

            APIDirective.extractIP(socketAddress) { ip =>
              entity(as[FinishedCheckpoint]) { fc =>
                optionalHeaderValueByName("ReplyTo") { replyToOpt =>
                  val baseHash = fc.checkpointCacheData.checkpointBlock.baseHash
                  logger.debug(
                    s"Handle finished checkpoint for cb: ${baseHash} and replyTo: $replyToOpt"
                  )

                  dao.metrics.incrementMetric("peerApiRXFinishedCheckpoint")

                  val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)
                  val bcs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

                  val callback = dao.checkpointAcceptanceService.acceptWithNodeCheck(fc)(cs).map { result =>
                    replyToOpt
                      .map(URI.create)
                      .map { u =>
                        logger.debug(
                          s"Making callback to: ${u.toURL} acceptance of cb: ${fc.checkpointCacheData.checkpointBlock.baseHash} performed $result"
                        )
                        makeCallback(u, FinishedCheckpointResponse(true))
                      }
                  }

                  val io = dao.snapshotService.getNextHeightInterval.flatMap { res =>
                    (res, fc.checkpointCacheData.height) match {
                      case (_, None) =>
                        IO { logger.warn(s"Missing height when accepting block $baseHash") } >>
                          StatusCodes.BadRequest.pure[IO]
                      case (2, _) => // TODO: hardcoded snapshot interval
                        callback.start(bcs) >> complete(StatusCodes.Accepted).pure[IO]
                      case (nextHeight, Some(Height(min, max))) if nextHeight > min =>
                        IO {
                          logger.debug(
                            s"Handle finished checkpoint for cb: ${fc.checkpointCacheData.checkpointBlock.baseHash} height condition not met next interval: ${nextHeight} received: ${fc.checkpointCacheData.height.get.min}"
                          )
                        } >> StatusCodes.Conflict.pure[IO]
                      case (_, _) =>
                        callback.start(bcs) >> StatusCodes.Accepted.pure[IO]
                    }
                  }

                  APIDirective.handle(io)(complete(_))
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

  private val blockBuildingRoundRoute =
    createRoute(ConsensusRoute.pathPrefix)(
      () =>
        new ConsensusRoute(dao.consensusManager, dao.snapshotService, dao.transactionService, dao.backend)
          .createBlockBuildingRoundRoutes()
    )

  val chunkSize = 100

  private[p2p] def mixedEndpoints(socketAddress: InetSocketAddress) =
    path("transaction") {
      put {
        entity(as[TransactionGossip]) { gossip =>
          logger.debug(s"Received transaction tx=${gossip.hash} with path=${gossip.path}")
          dao.metrics.incrementMetric("transactionRXByPeerAPI")

          implicit val random: Random = scala.util.Random

          /* TEMPORARY DISABLED todo: enable ignored tests as well org/constellation/p2p/PeerAPITest.scala:196
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
    } ~
      get {
        path("snapshot" / "obj" / "snapshot") {
          APIDirective.extractIP(socketAddress) { ip =>
            val snapshotHash: IO[Array[Byte]] =
              dao.snapshotService.recentSnapshotInfo.get.map{
                sni =>
                  val ser = KryoSerializer.serialize[String](sni.get.snapshot)
                  logger.warn(s"snapshot/obj/snapshot ${ser}")
                ser
              }
            APIDirective.handle[Array[Byte]](snapshotHash)(complete(_))
          }
        } ~ path("snapshot" / "obj" / "snapshotCBS") {
          APIDirective.extractIP(socketAddress) { ip =>
            val snapshotHash = dao.snapshotService.recentSnapshotInfo.get.map { sni =>
              logger.warn(s"snapshot/obj/snapshotCBS prev")
              val test: Array[Array[Byte]] = sni.get.checkpointBlocks
                .grouped(chunkSize)
                .map(t => chunkSerialize[String](t.toArray, "snapshotCBS"))
                .toArray
              logger.warn(s"snapshot/obj/snapshotCBS num snapshotCBS: ${sni.get.checkpointBlocks.length}")
              test
            }
            APIDirective.handle(snapshotHash)(complete(_))
          }
        } ~
          path("snapshot" / "obj" / "acceptedCBSinceSnapshot") {
            APIDirective.extractIP(socketAddress) { ip =>
              val snapshotHash = dao.snapshotService.recentSnapshotInfo.get.map { sni =>
                val test: Array[Array[Byte]] = sni.get.acceptedCBSinceSnapshot
                  .grouped(chunkSize)
                  .map(t => chunkSerialize(t.toArray, "acceptedCBSinceSnapshot"))
                  .toArray
                test
              }
              APIDirective.handle(snapshotHash)(complete(_))
            }
          } ~
          path("snapshot" / "obj" / "acceptedCBSinceSnapshotCache") {
            APIDirective.extractIP(socketAddress) { ip =>
              val snapshotHash = dao.snapshotService.recentSnapshotInfo.get.map { sni =>
                val test: Array[Array[Byte]] = sni.get.acceptedCBSinceSnapshotCache
                  .grouped(chunkSize)
                  .map(t => chunkSerialize(t.toArray, "acceptedCBSinceSnapshotCache"))
                  .toArray
                logger.warn(s"snapshot/obj/snapshotCBS ${sni.get.acceptedCBSinceSnapshotCache.length}")
                test
              }
              APIDirective.handle(snapshotHash)(complete(_))
            }
          } ~
          path("snapshot" / "obj" / "lastSnapshotHeight") {
            APIDirective.extractIP(socketAddress) { ip =>
              val snapshotHash = dao.snapshotService.recentSnapshotInfo.get
                .map(sni => KryoSerializer.serialize[Int](sni.get.lastSnapshotHeight))
              APIDirective.handle(snapshotHash)(complete(_))
            }
          } ~
          path("snapshot" / "obj" / "snapshotHashes") {
            APIDirective.extractIP(socketAddress) { ip =>
              val snapshotHash = dao.snapshotService.recentSnapshotInfo.get.map { sni =>
                val test: Array[Array[Byte]] =
                  sni.get.snapshotHashes
                    .grouped(chunkSize)
                    .map(t => chunkSerialize(t.toArray, "snapshotHashes"))
                    .toArray
                logger.warn(s"snapshot/obj/snapshotHashes ${sni.get.snapshotHashes.length}")
                test
              }
              APIDirective.handle(snapshotHash)(complete(_))
            }
          } ~
          path("snapshot" / "obj" / "addressCacheData") {
            APIDirective.extractIP(socketAddress) { ip =>
              val snapshotHash = dao.snapshotService.recentSnapshotInfo.get.map { sni =>
                val test: Array[Array[Byte]] =
                  sni.get.addressCacheData.toSeq.grouped(chunkSize).map(t => chunkSerialize(t.toArray, "tips")).toArray
                logger.warn(s"snapshot/obj/addressCacheData ${test.length}")
                test
              }
              APIDirective.handle(snapshotHash)(complete(_))
            }
          }~
          path("snapshot" / "obj" / "tips") {
            APIDirective.extractIP(socketAddress) { ip =>
              val snapshotHash = dao.snapshotService.recentSnapshotInfo.get.map { sni =>
                val test: Array[Array[Byte]] =
                  sni.get.tips.toSeq.grouped(chunkSize).map(t => chunkSerialize(t.toArray, "tips")).toArray
                logger.warn(s"snapshot/obj/tips ${sni.get.tips.toSeq.length}")
                test
              }
              APIDirective.handle(snapshotHash)(complete(_))
            }
          } ~
          path("snapshot" / "obj" / "snapshotCache") {
            APIDirective.extractIP(socketAddress) { ip =>
              val snapshotHash = dao.snapshotService.recentSnapshotInfo.get.map { sni =>
                val test: Array[Array[Byte]] =
                  sni.get.snapshotCache.grouped(chunkSize).map(t => chunkSerialize(t.toArray, "snapshotCache")).toArray
                test
              }
              APIDirective.handle(snapshotHash)(complete(_))
            }
          } ~
          path("snapshot" / "obj" / "lastAcceptedTransactionRef") {
            APIDirective.extractIP(socketAddress) { ip =>
              val snapshotHash = dao.snapshotService.recentSnapshotInfo.get.map { sni =>
                val test: Array[Array[Byte]] = sni.get.lastAcceptedTransactionRef
                  .grouped(chunkSize)
                  .map(t => chunkSerialize(t.toArray, "lastAcceptedTransactionRef"))
                  .toArray
                test
              }
              APIDirective.handle(snapshotHash)(complete(_))
            }
          } ~
          path("snapshot" / "info") {
            APIDirective.extractIP(socketAddress) { ip =>
              val getInfo =
                idLookup(ip)
                  .flatMap(
                    maybePeer =>
                      maybePeer.fold(IO(logger.warn(s"Unable to map ip: ${ip} to peer")))(
                        pd =>
                          // mwadon: Is it correct? Every time the node asks for "snapshot/info" it means SnapshotMisalignment?
                          dao.observationService
                            .put(Observation.create(pd.peerMetadata.id, SnapshotMisalignment())(dao.keyPair))
                            .void
                    )
                  )
                  .flatMap(
                    _ =>
                      dao.snapshotService.getSnapshotInfo.flatMap { info =>
                        info.acceptedCBSinceSnapshot.toList.traverse {//todo .diff on info.acceptedCBSinceSnapshotCache with acceptedCBSinceSnapshotCache from querying peer
                          dao.checkpointService.fullData(_)
                        }.flatMap {
                          cbs =>
                            val snapshotInfo = info.copy(acceptedCBSinceSnapshotCache = cbs.flatten)
                            val io = dao.snapshotService.recentSnapshotInfo.modify{
                              _ =>
                                logger.warn(s"snapshot/info recentSnapshotInfo updating")
                                (Some(snapshotInfo), Array.empty[Byte])
                            }
//                            logger.warn(s"snapshot/obj/snapshot is empty: ${dao.snapshotService.recentSnapshotInfo.get}")
                            //                      val snapshotInfoSer = EdgeProcessor.toSnapshotInfoSer(snapshotInfo)
//                      val serialziedSnapshot = KryoSerializer.serializeAnyRef(snapshotInfo).some

//                      logger.warn(s"snapshot/info KryoSerializer.serializeAnyRef snapshotInfo of hash ${info.snapshot.hash} contains: " +
//                        s"acceptedCBSinceSnapshot ifo - ${snapshotInfo.acceptedCBSinceSnapshot.length} " +
//                        s"acceptedCBSinceSnapshot SER - ${snapshotInfoSer.acceptedCBSinceSnapshot.length} " +
//                      s"snapshotHashes ifo - ${snapshotInfo.snapshotHashes.length}" +
//                        s"snapshotCache ifo - ${snapshotInfo.snapshotCache.length}" )
//                        s"snapshotCache SER - ${snapshotInfoSer.snapshotCache.length}")
//                        s"with size ${serialziedSnapshot.get.size}")
//                      logger.warn(s"snapshot/info KryoSerializer.serializeAnyRef for snapshotInfo: ${snapshotInfo} with size ${serialziedSnapshot.size}")
                            io
                        }
                    }
                  )

              APIDirective.handle(
                dao.cluster.getNodeState
                  .map(NodeState.canActAsDownloadSource)
                  .ifM(getInfo, IO.pure(none[Array[Byte]]))
              )(complete(_))
            }
          } ~
          path("trust") {
            APIDirective.handle(
              dao.trustManager.getPredictedReputation.flatMap { predicted =>
                if (predicted.isEmpty) dao.trustManager.getStoredReputation.map(TrustData)
                else TrustData(predicted).pure[IO]
              }
            )(complete(_))
          }
      }


  def routes(socketAddress: InetSocketAddress): Route =
    APIDirective.extractIP(socketAddress) { ip =>
      decodeRequest {
        encodeResponse {
          if (authEnabled) {
            authenticateBasic(realm = "basic realm", basicTokenAuthenticator) { _ =>
              peerApiRoutes(socketAddress, ip)
            }
          } else {
            peerApiRoutes(socketAddress, ip)
          }
        }
      }
    }

  private def peerApiRoutes(socketAddress: InetSocketAddress, ip: String): Route =
    signEndpoints(socketAddress) ~ commonEndpoints ~ batchEndpoints ~
      withSimulateTimeout(dao.simulateEndpointTimeout)(ConstellationExecutionContext.unbounded) {
        enforceKnownIP(ip) {
          postEndpoints(socketAddress) ~ mixedEndpoints(socketAddress) ~ blockBuildingRoundRoute
        }
      }

  private[p2p] def makeCallback(u: URI, entity: AnyRef): Future[Response[Unit]] =
    APIClient(u.getHost, u.getPort)(dao.backend, dao)
      .postNonBlockingUnit(u.getPath, entity)

  private def createRoute(path: String)(routeFactory: () => Route): Route =
    pathPrefix(path) {
      handleExceptions(exceptionHandler) {
        routeFactory()
      }
    }

  def idLookup(host: String): IO[Option[PeerData]] =
    dao.cluster.getPeerData(host)
}

case class IpIdMappingException(ip: String, port: Int) extends Exception(s"Unable to map ip: $ip to Id")
