package org.constellation

import java.io.{StringWriter, Writer}
import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{entity, path, _}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.pattern.CircuitBreaker
import akka.util.Timeout
import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import cats.effect.IO
import cats.implicits._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import org.constellation.api.TokenAuthenticator
import org.constellation.checkpoint.CheckpointBlockValidator.ValidationResult
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.domain.trust.TrustData
import org.constellation.keytool.KeyUtils
import org.constellation.p2p.{ChangePeerState, Download, SetStateResult}
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.NodeType.NodeType
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.util._
import org.json4s.native.Serialization
import org.json4s.{JValue, native}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

case class PeerMetadata(
  host: String,
  httpPort: Int,
  id: Id,
  nodeState: NodeState = NodeState.Ready,
  timeAdded: Long = System.currentTimeMillis(),
  auxHost: String = "",
  auxAddresses: Seq[String] = Seq(), // for testing multi key address partitioning
  nodeType: NodeType = NodeType.Full,
  resourceInfo: ResourceInfo
)

case class ResourceInfo(
  maxMemory: Long = Runtime.getRuntime.maxMemory(),
  cpuNumber: Int = Runtime.getRuntime.availableProcessors(),
  diskUsableBytes: Long
)

case class RemovePeerRequest(host: Option[HostPort] = None, id: Option[Id] = None)

case class UpdatePassword(password: String)

object ProcessingConfig {

  val testProcessingConfig = ProcessingConfig(
    maxWidth = 10,
    maxMemPoolSize = 1000,
    minPeerTimeAddedSeconds = 5,
    roundsPerMessage = 1,
    leavingStandbyTimeout = 3
  )
}

case class ProcessingConfig(
  maxWidth: Int = 10,
  maxTipUsage: Int = 2,
  maxTXInBlock: Int = 50,
  maxMessagesInBlock: Int = 1,
  peerInfoTimeout: Int = 3,
  snapshotTriggeringTimeSeconds: Int = 5,
  formUndersizedCheckpointAfterSeconds: Int = 30,
  numFacilitatorPeers: Int = 2,
  metricCheckInterval: Int = 10,
  maxMemPoolSize: Int = 35000,
  minPeerTimeAddedSeconds: Int = 30,
  maxActiveTipsAllowedInMemory: Int = 1000,
  maxAcceptedCBHashesInMemory: Int = 50000,
  peerHealthCheckInterval: Int = 30,
  peerDiscoveryInterval: Int = 60,
  formCheckpointTimeout: Int = 60,
  maxFaucetSize: Int = 1000,
  roundsPerMessage: Int = 10,
  recentSnapshotNumber: Int = 30,
  maxInvalidSnapshotRate: Int = 51,
  txGossipingFanout: Int = 2,
  leavingStandbyTimeout: Int = 30
) {}

case class ChannelUIOutput(channels: Seq[String])

case class ChannelValidationInfo(channel: String, valid: Boolean)

case class BlockUIOutput(
  id: String,
  height: Long,
  parents: Seq[String],
  channels: Seq[ChannelValidationInfo]
)

class API()(implicit system: ActorSystem, val timeout: Timeout, val dao: DAO)
    extends Json4sSupport
    with ServeUI
    with CommonEndpoints
    with ConfigEndpoints
    with StrictLogging
    with TokenAuthenticator {

  import dao._

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

  private val authEnabled: Boolean = ConfigUtil.getAuthEnabled

  val getEndpoints: Route =
    extractClientIP { clientIP =>
      get {
        pathPrefix("cluster") {
          path("info") {
            APIDirective.handle(dao.cluster.clusterNodes())(complete(_))
          }
        } ~
          path("channels") {
            complete(dao.threadSafeMessageMemPool.activeChannels.keys.toSeq)
          } ~
          pathPrefix("data") {
            path("channels") {
              complete(ChannelUIOutput(dao.threadSafeMessageMemPool.activeChannels.keys.toSeq))
            } ~
              path("channel" / Segment / "info") { channelId =>
                complete(dao.channelService.lookup(channelId).unsafeRunSync().map { cmd =>
                  SingleChannelUIOutput(
                    cmd.channelOpen,
                    cmd.totalNumMessages,
                    cmd.last25MessageHashes,
                    cmd.genesisMessageMetadata.channelMessage.signedMessageData.signatures.signatures.head.address
                  )
                })
              } ~
              path("channel" / Segment / "schema") { channelId =>
                complete(
                  dao.channelService
                    .lookup(channelId)
                    .unsafeRunSync()
                    .flatMap { cmd =>
                      cmd.channelOpen.jsonSchema
                    }
                    .map { schemaStr =>
                      import org.json4s.native.JsonMethods._
                      pretty(render(parse(schemaStr)))
                    }
                )
              }
          } ~
          pathPrefix("data") {
            path("channels") {
              complete(ChannelUIOutput(dao.threadSafeMessageMemPool.activeChannels.keys.toSeq))
            } ~
              path("channel" / Segment / "info") { channelId =>
                complete(dao.channelService.lookup(channelId).unsafeRunSync().map { cmd =>
                  SingleChannelUIOutput(
                    cmd.channelOpen,
                    cmd.totalNumMessages,
                    cmd.last25MessageHashes,
                    cmd.genesisMessageMetadata.channelMessage.signedMessageData.signatures.signatures.head.address
                  )
                })
              } ~
              path("channel" / Segment / "schema") { channelId =>
                complete(
                  dao.channelService
                    .lookup(channelId)
                    .unsafeRunSync()
                    .flatMap { cmd =>
                      cmd.channelOpen.jsonSchema
                    }
                    .map { schemaStr =>
                      import org.json4s.native.JsonMethods._
                      pretty(render(parse(schemaStr)))
                    }
                )
              } ~
              path("graph") { // Debugging / mockup for peer graph
                getFromResource("sample_data/dag.json")
              } ~
              path("blocks") {

                val blocks = dao.recentBlockTracker.getAll.toSeq
                //dao.threadSafeTipService.acceptedCBSinceSnapshot.flatMap{dao.checkpointService.getSync}
                complete(blocks.map { ccd =>
                  val cb = ccd.checkpointBlock

                  BlockUIOutput(
                    cb.soeHash,
                    ccd.height.get.min,
                    cb.parentSOEHashes,
                    cb.messages.map {
                      _.signedMessageData.data.channelId
                    }.distinct.map { channelId =>
                      ChannelValidationInfo(channelId, true)
                    }
                  )
                })
              }
          } ~
          path("messageService" / Segment) { channelId =>
            APIDirective.handle(dao.messageService.lookup(channelId))(complete(_))
          } ~
          path("channelKeys") {
            APIDirective.handle(dao.channelService.toMap().map(_.keys.toSeq))(complete(_))
          } ~
          path("channel" / "genesis" / Segment) { channelId =>
            APIDirective.handle(dao.channelService.lookup(channelId))(complete(_))
          } ~
          path("channel" / Segment) { channelHash =>
            def makeProof(cmd: ChannelMessageMetadata, storedSnapshot: StoredSnapshot) = {
              val blocksInSnapshot = storedSnapshot.snapshot.checkpointBlocks.toList
              val blockHashForMessage = cmd.blockHash.get

              if (!blocksInSnapshot.contains(blockHashForMessage)) {
                logger.error("Message block hash not in snapshot")
              }

              val blockProof = MerkleTree(blocksInSnapshot).createProof(blockHashForMessage)

              val block = storedSnapshot.checkpointCache.filter {
                _.checkpointBlock.baseHash == blockHashForMessage
              }.head.checkpointBlock

              val messageProofInput = block.transactions.map {
                _.hash
              } ++ block.messages.map {
                _.signedMessageData.signatures.hash
              }

              val messageProof = MerkleTree(messageProofInput.toList)
                .createProof(cmd.channelMessage.signedMessageData.signatures.hash)

              ChannelProof(
                cmd,
                blockProof,
                messageProof
              )
            }

            val proof = for {
              msg <- dao.messageService.memPool.lookup(channelHash)
              channelRes = Snapshot.findLatestMessageWithSnapshotHash(0, msg)

              res <- if (channelRes.isDefined || msg.isEmpty) {
                channelRes.pure[IO]
              } else {
                dao.messageService
                  .lookup(msg.get.channelMessage.signedMessageData.hash)
                  .map(Snapshot.findLatestMessageWithSnapshotHash(0, _))
              }

              exists <- res.flatMap(_.snapshotHash.map(dao.snapshotService.exists)).sequence.map(_.forall(_ == true))

              proof = if (exists) {
                res.flatMap { cmd =>
                  cmd.snapshotHash.flatMap { snapshotHash =>
                    tryWithMetric(
                      {
                        import better.files._
                        KryoSerializer.deserializeCast[StoredSnapshot](File(dao.snapshotPath, snapshotHash).byteArray)
                      },
                      "readSnapshotForMessage"
                    ).toOption.map(makeProof(cmd, _))
                  }
                }
              } else None
            } yield proof

            APIDirective.handle(proof)(complete(_))
          } ~
          path("messages") {
            APIDirective.handle(IO {
              dao.channelStorage.getLastNMessages(20)
            })(complete(_))
          } ~
          path("messages" / Segment) { channelId =>
            APIDirective.handle(IO {
              dao.channelStorage.getLastNMessages(20, Some(channelId))
            })(complete(_))
          } ~
          path("restart") { // TODO: Revisit / fix
            System.exit(0)
            complete(StatusCodes.OK)
          } ~
          path("setKeyPair") { // TODO: Change to keys/set - update ui call
            parameter('keyPair) { kpp =>
              logger.debug("Set key pair " + kpp)
              val res = if (kpp.length > 10) {
                val rr = Try {
                  dao.updateKeyPair(kpp.x[KeyPair])
                  StatusCodes.OK
                }.getOrElse(StatusCodes.BadRequest)
                rr
              } else StatusCodes.BadRequest
              complete(res)
            }
          } ~
          path("buildInfo") {
            complete(BuildInfo.toMap)
          } ~
          path("metrics") {
            val response = MetricsResult(
              dao.metrics.getMetrics
            )
            complete(response)
          } ~
          path("micrometer-metrics") {
            val writer: Writer = new StringWriter()
            TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples())
            complete(writer.toString)
          } ~
          path("makeKeyPair") {
            val pair = KeyUtils.makeKeyPair()
            wallet :+= pair
            complete(pair)
          } ~
          path("makeKeyPairs" / IntNumber) { numPairs =>
            val pair = Seq.fill(numPairs) {
              KeyUtils.makeKeyPair()
            }
            wallet ++= pair
            complete(pair)
          } ~
          path("wallet") {
            complete(wallet)
          } ~
          path("selfAddress") {
            complete(id.address)
          } ~
          path("nodeKeyPair") {
            complete(keyPair)
          } ~
          path("hasGenesis") {
            if (dao.genesisObservation.isDefined) {
              complete(StatusCodes.OK)
            } else {
              complete(StatusCodes.NotFound)
            }
          } ~
          path("dashboard") {
            val txs = dao.transactionService.getLast20Accepted.unsafeRunSync()
            val self = Node(
              dao.selfAddressStr,
              dao.externalHostString,
              dao.externalPeerHTTPPort
            )

            val peerMap = dao.peerInfo.map {
              _.toSeq.map {
                case (id, pd) => Node(id.address, pd.peerMetadata.host, pd.peerMetadata.httpPort)
              } :+ self
            }

            complete(
              Map(
                "peers" -> peerMap.unsafeRunSync(),
                "transactions" -> txs
              )
            )

          } ~
          path("trust") {
            APIDirective.handle(
              dao.trustManager.getPredictedReputation.flatMap { predicted =>
                if (predicted.isEmpty) dao.trustManager.getStoredReputation.map(TrustData)
                else TrustData(predicted).pure[IO]
              }
            )(complete(_))
          } ~
          path("storedReputation") {
            APIDirective.handle(
              dao.trustManager.getStoredReputation.map(TrustData)
            )(complete(_))
          } ~
          path("predictedReputation") {
            APIDirective.handle(
              dao.trustManager.getPredictedReputation.map(TrustData)
            )(complete(_))
          }
      }
    }

  val postEndpoints =
    post {
      pathPrefix("channel") {
        path("open") {
          entity(as[ChannelOpen]) { request =>
            val ioa = IO
              .fromFuture(IO(ChannelMessage.createGenesis(request)))
              .handleErrorWith(_ => IO(ChannelOpenResponse("Failed to open channel")))

            APIDirective.handle(ioa)(complete(_))
          }
        } ~
          path("send") {
            entity(as[ChannelSendRequest]) { send =>
              val ioa = IO
                .fromFuture(IO(ChannelMessage.createMessages(send)))
                .map(_.json)
                .handleErrorWith(_ => IO(ChannelSendResponse("Failed to create messages", Seq()).json))
              APIDirective.handle(ioa)(complete(_))
            }
          } ~
          path("send" / "json") {
            entity(as[ChannelSendRequestRawJson]) { send: ChannelSendRequestRawJson =>
              import constellation._
              val amended = ChannelSendRequest(
                send.channelId,
                send.messages.x[Seq[JValue]].map { _.json }
              )

              val ioa = IO
                .fromFuture(IO(ChannelMessage.createMessages(amended)))
                .map(_.json)
                .handleErrorWith(_ => IO(ChannelOpenResponse("Failed to send raw json").json))
              APIDirective.handle(ioa)(complete(_))
            }
          }
      } ~
        pathPrefix("peer") {
          path("remove") {
            entity(as[ChangePeerState]) { e =>
              APIDirective.handle(dao.cluster.setNodeStatus(e.id, e.state).map(_ => StatusCodes.OK))(complete(_))
            }
          } ~
            path("add") {
              entity(as[HostPort]) { hp =>
                APIDirective.handle(
                  dao.cluster
                    .attemptRegisterPeer(hp)
                    .map(result => {
                      logger.debug(s"Add Peer Request: $hp. Result: $result")
                      StatusCode.int2StatusCode(result.code)
                    })
                )(complete(_))
              }
            }
        } ~
        pathPrefix("download") {
          path("start") {
            Future { Download.download()(dao, ConstellationExecutionContext.bounded) }(
              ConstellationExecutionContext.bounded
            )
            complete(StatusCodes.OK)
          }
        } ~
        pathPrefix("config") {
          path("update") {
            entity(as[ProcessingConfig]) { cu =>
              dao.nodeConfig = dao.nodeConfig.copy(processingConfig = cu)
              complete(StatusCodes.OK)
            }
          }
        } ~
        path("ready") {
          def changeStateToReady: IO[SetStateResult] = dao.cluster.compareAndSet(NodeState.initial, NodeState.Ready)

          APIDirective.onHandle(changeStateToReady) {
            case Success(SetStateResult(_, true))  => complete(StatusCodes.OK)
            case Success(SetStateResult(_, false)) => complete(StatusCodes.Conflict)
            case Failure(error) =>
              logger.error(s"Can't transition state to ${error.getMessage}", error)
              complete(StatusCodes.InternalServerError)
          }

        } ~
        path("peerHealthCheck") {
          val resetTimeout = 1.second
          val breaker = new CircuitBreaker(system.scheduler, maxFailures = 1, callTimeout = 15.seconds, resetTimeout)(
            ConstellationExecutionContext.unbounded
          )

          val response =
            dao.cluster.broadcast(_.getStringIO("health")(IO.contextShift(ConstellationExecutionContext.bounded)))

          onCompleteWithBreaker(breaker)(response.unsafeToFuture) {
            case Success(idMap) =>
              val res = idMap.map {
                case (id, validatedResp) =>
                  id -> validatedResp.exists(_.isSuccess)
              }.toSeq

              complete(res)

            case Failure(e) =>
              logger.warn("Failed to get peer health", e)
              complete(StatusCodes.InternalServerError)
          }
        } ~
        pathPrefix("genesis") {
          path("create") {
            entity(as[Seq[AccountBalance]]) { balances =>
              logger.info(s"genesis created with balances ${balances}")
              val go = Genesis.createGenesisObservation(balances)
              complete(go)
            }
          } ~
            path("accept") {
              entity(as[GenesisObservation]) { go =>
                Genesis.acceptGenesis(go, setAsTips = true)
                // TODO: Report errors and add validity check
                complete(StatusCodes.OK)
              }
            }
        } ~
        path("balance") {
          entity(as[String]) { address =>
            logger.info(s"balance lookup for address ${address}")
            val io = addressService
              .lookup(address)
              .map(
                res =>
                  res
                    .map(_.balance.toString)
                    .getOrElse("0")
              )

            APIDirective.handle(io)(complete(_))
          }
        } ~
        path("transaction") {
          entity(as[Transaction]) { transaction =>
            logger.info(s"send transaction to address ${transaction.hash}")
            val io = checkpointBlockValidator
              .singleTransactionValidation(transaction)
              .flatMap {
                case Invalid(e) => IO { e.toString() }
                case Valid(tx)  => transactionService.put(TransactionCacheData(tx)).map(_.hash)
              }
            APIDirective.handle(io)(complete(_))
          }
        } ~ path("send") {
        entity(as[SendToAddress]) { sendRequest =>
          logger.info(s"send transaction to address $sendRequest")
          val io = transactionChainService
            .createAndSetLastTransaction(
              dao.selfAddressStr,
              sendRequest.dst,
              sendRequest.amountActual,
              dao.keyPair,
              false
            )
            .flatMap(tx => dao.transactionService.put(TransactionCacheData(tx)))
            .map(_.hash)

          APIDirective.handle(io)(complete(_))
        }
      } ~ path("restore") {
        APIDirective.onHandleEither(dao.rollbackService.validateAndRestore().value) {
          case Success(value) => complete(StatusCodes.OK)
          case Failure(error) =>
            logger.error(s"Restored error ${error}")
            complete(StatusCodes.InternalServerError)
        }
      } ~
        path("addPeer") {
          entity(as[PeerMetadata]) { pm =>
            (IO
              .contextShift(ConstellationExecutionContext.bounded)
              .shift >> dao.cluster.addPeerMetadata(pm)).unsafeRunAsyncAndForget

            complete(StatusCodes.OK)
          }
        } ~
        path("ip") {
          entity(as[String]) { externalIp =>
            var ipp: String = ""
            val addr =
              externalIp.replaceAll('"'.toString, "").split(":") match {
                case Array(ip, port) =>
                  ipp = ip
                  import better.files._
                  nodeConfig = nodeConfig.copy(hostName = ip)
                  file"${ConstellationNode.LocalConfigFile}".write(LocalNodeConfig(ip).json)
                  new InetSocketAddress(ip, port.toInt)
                case a @ _ => {
                  logger.debug(s"Unmatched Array: $a")
                  throw new RuntimeException(s"Bad Match: $a")
                }
              }
            logger.debug(s"Set external IP RPC request $externalIp $addr")
            dao.metrics.updateMetric("externalHost", dao.externalHostString)
            complete(StatusCodes.OK)
          }
        } ~
        path("reputation") {
          entity(as[Seq[UpdateReputation]]) { ur =>
            ur.foreach { r =>
              r.secretReputation.foreach {
                dao.secretReputation(r.id) == _
              }
              r.publicReputation.foreach {
                dao.publicReputation(r.id) == _
              }
            }
            complete(StatusCodes.OK)
          }
        }
    }

  private val noAuthRoutes =
    get {
      // TODO: revisit
      path("health") {
        complete(StatusCodes.OK)
      } ~
        path("favicon.ico") {
          getFromResource("ui/img/favicon.ico")
        }
    }

  def mainRoutes(socketAddress: InetSocketAddress): Route = cors() {
    decodeRequest {
      encodeResponse {
        getEndpoints ~ postEndpoints ~ configEndpoints ~ jsRequest ~ imageRoute ~ commonEndpoints ~ batchEndpoints ~ serveMainPage
      }
    }
  }

  def routes(socketAddress: InetSocketAddress): Route =
    APIDirective.extractIP(socketAddress) { ip =>
      if (authEnabled) {
        authenticateBasic(realm = "basic realm", basicTokenAuthenticator) { _ =>
          noAuthRoutes ~ mainRoutes(socketAddress)
        }
      } else {
        noAuthRoutes ~ mainRoutes(socketAddress)
      }
    }
}
