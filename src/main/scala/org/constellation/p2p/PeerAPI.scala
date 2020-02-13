package org.constellation.p2p

import java.net.{InetSocketAddress, URI}

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.util.Timeout
import org.constellation.rollback.RollbackLoader
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.scalalogging.StrictLogging
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.CustomDirectives.IPEnforcer
import org.constellation.api.TokenAuthenticator
import org.constellation.serializer.KryoSerializer.{chunkDeSerialize, chunkSerialize}
import org.constellation.domain.redownload.RedownloadService
import org.constellation.consensus.{ConsensusRoute, _}
import org.constellation.domain.trust.TrustData
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.storage.SnapshotService
import org.constellation.util._
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO, ResourceInfo}
import org.json4s.native
import org.json4s.native.Serialization

import scala.concurrent.Future
import scala.util.Random

case class PeerAuthSignRequest(salt: Long)

case class PeerRegistrationRequest(
  host: String,
  port: Int,
  id: Id,
  resourceInfo: ResourceInfo,
  isSimulation: Boolean = false
)

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
              APIDirective.handle(dao.cluster.pendingRegistration(ip, request, request.isSimulation))(
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
            complete(dao.peerRegistrationRequest()) // Include status also
          }
        }
      }

  private[p2p] def postEndpoints(socketAddress: InetSocketAddress) =
    post {
      pathPrefix("snapshot" / "info") {
        pathEnd {
          entity(as[(String, Array[Array[Byte]])]) {
            case (address, curCheckpointHashes) =>
              val deSerCheckpointHashes =
                curCheckpointHashes.flatMap(chunkDeSerialize[Seq[String]](_, "snapshot/info/curCheckpointHashes"))
              val getInfo = dao.snapshotService.getSnapshotInfo.flatMap { info =>
                val checkpointsToGet = info.acceptedCBSinceSnapshotHashes.toList.diff(deSerCheckpointHashes.toList)
                checkpointsToGet.traverse {
                  dao.checkpointService.fullData(_)
                }.flatMap { cbs =>
                  val infoSer = info.copy(acceptedCBSinceSnapshotCache = cbs.flatten).toSnapshotInfoSer()
                  val res = dao.snapshotService.recentSnapshotInfo
                    .put(address, infoSer)
                    .flatTap(_ => IO { logger.warn(s"snapshot/info recentSnapshotInfo updating") })
                    .map(_ => Array.empty[Byte].some)
                  res
                }
              }
              APIDirective.handle {
                dao.cluster.getNodeState
                  .map(NodeState.canActAsRedownloadSource)
                  .ifM(getInfo, IO.pure(None))
              } {
                case None => complete(StatusCodes.NotFound)
                case _    => complete(StatusCodes.OK, Array()) //need Array() here, Array.empty marshalls as List
              }
          }
        } ~
          path(LongNumber) { height =>
            entity(as[(String, Array[Array[Byte]])]) {
              case (address, curCheckpointHashes) =>
                val curSnap = dao.snapshotService.storedSnapshot.get.unsafeRunSync()
                if (height == curSnap.height) {
                  val deSerCheckpointHashes =
                    curCheckpointHashes.flatMap(chunkDeSerialize[Seq[String]](_, "snapshot/info/curCheckpointHashes"))
                  logger.debug(
                    s"snapshot/info/${height} from node address: ${address} - height == curSnap.height: true  - deSerCheckpointHashes.size ${deSerCheckpointHashes.size}"
                  )
                  val getInfo = dao.snapshotService.getSnapshotInfo.flatMap { info =>
                    val checkpointsToGet = info.acceptedCBSinceSnapshotHashes.toList.diff(deSerCheckpointHashes.toList)
                    checkpointsToGet.traverse {
                      dao.checkpointService.fullData(_)
                    }.flatMap { cbs =>
                      val infoSer = info.copy(acceptedCBSinceSnapshotCache = cbs.flatten).toSnapshotInfoSer()
                      val res = dao.snapshotService.recentSnapshotInfo
                        .put(address, infoSer)
                        .flatTap(_ => IO { logger.warn(s"snapshot/info recentSnapshotInfo updating") })
                        .map(_ => Array.empty[Byte].some)
                      val testPut = dao.snapshotService.recentSnapshotInfo.lookup(address)
                      val check = (res >> testPut).unsafeRunSync()
                      val didSave = (check.get == infoSer)
                      logger.debug(s"didSave ${didSave} - for address $address with snapshotHashes ${info
                        .copy(acceptedCBSinceSnapshotCache = cbs.flatten)
                        .snapshotHashes} - for node ${dao.id.address}")
                      res
                    }
                  }
                  APIDirective.handle {
                    dao.cluster.getNodeState
                      .map(NodeState.canActAsRedownloadSource)
                      .ifM(getInfo, IO.pure(None))
                  } {
                    case None => complete(StatusCodes.NotFound)
                    case _    => complete(StatusCodes.OK, Array())
                  }
                } else {
                  val heightPrefix = SnapshotService.snapshotInfoFileHeightPrefix(height)
                  val snapshotInfoDirectories = dao.snapshotInfoPath.collectChildren(_.isDirectory).toList //need to call toList here
                  val infoSerOpt = snapshotInfoDirectories.find { d =>
                    val snapshotInfoDir = d.pathAsString.split("/").reverse.head.startsWith(heightPrefix)
                    logger.debug(
                      s"snapshot/info/${height} from node address: ${address} - snapshotInfoDir - ${snapshotInfoDir} - heightPrefix ${heightPrefix} - snapshotInfoDirectories ${snapshotInfoDirectories}"
                    )
                    snapshotInfoDir
                  }.map { f =>
                    logger.debug(
                      s"snapshot/info/${height} from node address: ${address} - foundDir - ${f.pathAsString} - heightPrefix ${heightPrefix} - snapshotInfoDirectories ${snapshotInfoDirectories}"
                    )
                    RollbackLoader.loadSnapshotInfoSer(f.pathAsString) //todo, filter checkpoint cache with curCheckpointHashes as above ?
                  }
                  val storeInfo = infoSerOpt.fold(IO { Array.empty[Byte] }) { infoSer =>
                    val res = dao.snapshotService.recentSnapshotInfo
                      .put(address, infoSer)
                      .flatTap(_ => IO { logger.warn(s"snapshot/info recentSnapshotInfo updating") })
                      .map(_ => Array.empty[Byte])
                    val testPut = dao.snapshotService.recentSnapshotInfo.lookup(address)
                    val check = (res >> testPut).unsafeRunSync()
                    val didSave = (check.get == infoSer)
                    logger.debug(s"didSave ${didSave} - for requesting address $address - on node ${dao.id.address}")
                    res
                  }
                  APIDirective.handle {
                    dao.cluster.getNodeState
                      .map(NodeState.canActAsRedownloadSource)
                      .ifM(storeInfo, IO.pure(None))
                  } {
                    case None => complete(StatusCodes.NotFound)
                    case _    => complete(StatusCodes.OK, Array())
                  }
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
            // TODO: Chain
            if (sendRequest.amountActual < (dao.processingConfig.maxFaucetSize * Schema.NormalizationFactor) &&
                dao.addressService
                  .lookup(dao.selfAddressStr)
                  .unsafeRunSync()
                  .map { _.balance }
                  .getOrElse(0L) > (dao.processingConfig.maxFaucetSize * Schema.NormalizationFactor * 10)) {

              val tx = dao.transactionChainService
                .createAndSetLastTransaction(
                  dao.selfAddressStr,
                  sendRequest.dst,
                  sendRequest.amountActual,
                  dao.keyPair,
                  false,
                  normalized = sendRequest.normalized
                )
                .flatMap { tx =>
                  logger.debug(s"faucet create transaction with hash: ${tx.hash} send to address $sendRequest")
                  dao.metrics.incrementMetric("faucetRequest")
                  dao.transactionService.put(TransactionCacheData(tx))
                }
                .map(_.hash)

              APIDirective.handle(tx)(complete(_))
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

                  // TODO: makeCallback returns side-effectful Future inside IO.map
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
    } //todo move "snapshot" / "obj" / "snapshot" from mixed endpoints to post endpoints?
  private val blockBuildingRoundRoute =
    createRoute(ConsensusRoute.pathPrefix)(
      () =>
        new ConsensusRoute(dao.consensusManager, dao.snapshotService, dao.transactionService, dao.backend)
          .createBlockBuildingRoundRoutes()
    )

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
    } ~ post {
      path("snapshot" / "obj" / "snapshot") {
        entity(as[String]) { ip =>
          val snapshotHash: IO[Array[Array[Byte]]] =
            dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map { sni =>
              logger.debug(s"snapshot/obj/snapshot - for address $ip - sni ${sni}")
              val ser = sni.get.snapshot
              logger.debug(s"snapshot/obj/snapshot ser - $ser for address $ip")
              ser
            }
          APIDirective.handle(snapshotHash)(complete(_))
        }
      } ~ path("snapshot" / "obj" / "snapshotCBs") {
        entity(as[String]) { ip =>
          val snapshotCBS = dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map { sni =>
            logger.debug(s"snapshot/obj/snapshotCBS - for address $ip - sni ${sni}")
            val res = sni.get.snapshotCheckpointBlocks
            logger.debug(s"snapshot/obj/snapshotCBS num snapshotCBS: ${res.length}")
            res
          }
          APIDirective.handle(snapshotCBS)(complete(_))
        }
      } ~
        path("snapshot" / "obj" / "storedSnapshotCheckpointBlocks") {
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/storedSnapshotCheckpointBlocks for address: $ip")
            val storedSnapshotCheckpointBlocks =
              dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map { sni =>
                val res = sni.get.storedSnapshotCheckpointBlocks
                logger
                  .debug(
                    s"snapshot/obj/storedSnapshotCheckpointBlocks num storedSnapshotCheckpointBlocks: ${res.length}"
                  )
                res
              }
            APIDirective.handle(storedSnapshotCheckpointBlocks)(complete(_))
          }
        } ~
        path("snapshot" / "obj" / "acceptedCBSinceSnapshot") {
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/acceptedCBSinceSnapshot for address: $ip")
            val acceptedCBSinceSnapshot =
              dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map { sni =>
                val res = sni.get.acceptedCBSinceSnapshotHashes
                logger.debug(s"snapshot/obj/acceptedCBSinceSnapshot num acceptedCBSinceSnapshot: ${res.length}")
                res
              }
            APIDirective.handle(acceptedCBSinceSnapshot)(complete(_))
          }
        } ~
        path("snapshot" / "obj" / "acceptedCBSinceSnapshotCache") {
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/acceptedCBSinceSnapshotCache for address: $ip")
            val acceptedCBSinceSnapshotCache =
              dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map { sni =>
                val res = sni.get.acceptedCBSinceSnapshotCache
                logger.debug(s"snapshot/obj/acceptedCBSinceSnapshotCache ${res.length}")
                res
              }
            APIDirective.handle(acceptedCBSinceSnapshotCache)(complete(_))
          }
        } ~
        path("snapshot" / "obj" / "awaiting") {
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/awaiting for address: $ip")
            val awaiting = dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map { sni =>
              val res: Array[Array[Byte]] = sni.get.awaitingCbs
              logger.debug(s"snapshot/obj/awaiting ${res.length}")
              res
            }
            APIDirective.handle(awaiting)(complete(_))
          }
        } ~
        path("snapshot" / "obj" / "lastSnapshotHeight") {
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/lastSnapshotHeight for address: $ip")
            val lastSnapshotHeight = dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map {
              sni =>
                val res = sni.get.lastSnapshotHeight
                logger.debug(s"snapshot/obj/lastSnapshotHeight ${res.length}")
                res
            }
            APIDirective.handle(lastSnapshotHeight)(complete(_))
          }
        } ~
        path("snapshot" / "obj" / "snapshotHashes") {
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/snapshotHashes for address: $ip")
            val snapshotHash = dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map {
              sni =>
                val res: Array[Array[Byte]] = sni.get.snapshotHashes
                logger.debug(s"snapshot/obj/snapshotHashes ${res.length}")
                res
            }
            APIDirective.handle(snapshotHash)(complete(_))
          }
        } ~
        path("snapshot" / "obj" / "snapshotPublicReputation") {
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/snapshotPublicReputation for address: $ip")
            val snapshotPublicReputation =
              dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map { sni =>
                val res = sni.get.snapshotPublicReputation
                logger.debug(s"snapshot/obj/snapshotPublicReputation ${res.length}")
                res
              }
            APIDirective.handle(snapshotPublicReputation)(complete(_))
          }
        } ~
        path("snapshot" / "obj" / "addressCacheData") {
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/addressCacheData for address: $ip")
            val addressCacheData = dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map {
              sni =>
                val res: Array[Array[Byte]] = sni.get.addressCacheData
                logger.debug(s"snapshot/obj/addressCacheData ${res.length}")
                res
            }
            APIDirective.handle(addressCacheData)(complete(_))
          }
        } ~
        path("snapshot" / "obj" / "tips") {
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/tips for address: $ip")
            val tips = dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map { sni =>
              val res: Array[Array[Byte]] = sni.get.tips
              logger.debug(s"snapshot/obj/tips ${res.length}")
              res
            }
            APIDirective.handle(tips)(complete(_))
          }
        } ~
        path("snapshot" / "obj" / "snapshotCache") { //todo filter here too
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/snapshotCache for address: $ip")
            val snapshotCache = dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map {
              sni =>
                val res: Array[Array[Byte]] = sni.get.snapshotCache
                logger.debug(s"snapshot/obj/snapshotCache ${res.length}")
                res
            }
            APIDirective.handle(snapshotCache)(complete(_))
          }
        } ~
        path("snapshot" / "obj" / "lastAcceptedTransactionRef") {
          entity(as[String]) { ip =>
            logger.debug(s"snapshot/obj/lastAcceptedTransactionRef for address: $ip")
            val lastAcceptedTransactionRef =
              dao.snapshotService.recentSnapshotInfo.lookup(ip.replaceAll("^\"+|\"+$", "")).map { sni =>
                val res: Array[Array[Byte]] = sni.get.lastAcceptedTransactionRef
                logger.debug(s"snapshot/obj/lastAcceptedTransactionRef ${res.length}")
                res
              }
            APIDirective.handle(lastAcceptedTransactionRef)(complete(_))
          }
        }
    } ~ get {
      path("snapshot" / "stored") {
        val storedSnapshots = dao.snapshotStorage.getSnapshotHashes
        val recentSnapshot = dao.snapshotService.storedSnapshot.get.map(_.snapshot.hash)
        val hashes = storedSnapshots.flatMap { h =>
          recentSnapshot.map(hh => List(hh) ++ h)
        }

        APIDirective.handle(hashes)(complete(_))
      } ~
        path("snapshot" / "own") {
          val snapshots = dao.redownloadService.getLocalSnapshots()
          val chunkedSnaps = snapshots.map { snapMap =>
            snapMap
              .grouped(KryoSerializer.chunkSize)
              .map(t => chunkSerialize(t.toSeq, RedownloadService.fetchSnapshotProposals))
              .toArray
          }
          APIDirective.handle(chunkedSnaps)(complete(_))
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
