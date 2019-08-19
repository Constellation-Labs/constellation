package org.constellation

import java.io.{StringWriter, Writer}
import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{entity, path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.Credentials
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.pattern.CircuitBreaker
import akka.util.Timeout
import cats.effect.IO
import cats.implicits._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.crypto.KeyUtils
import org.constellation.p2p.{ChangePeerState, Download, SetNodeStatus}
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.NodeType.NodeType
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util._
import org.json4s.native.Serialization
import org.json4s.{JValue, native}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
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
    numFacilitatorPeers = 2,
    maxTransactionsPerRound = 2,
    emptyTransactionsRounds = 2,
    amountTransactionsRounds = 2,
    metricCheckInterval = 10,
    maxWidth = 4,
    maxMemPoolSize = 15,
    minPeerTimeAddedSeconds = 1,
    snapshotInterval = 2,
    snapshotHeightInterval = 2,
    snapshotHeightDelayInterval = 1,
    roundsPerMessage = 1,
    leavingStandbyTimeout = 3
  )
}

case class ProcessingConfig(
  maxWidth: Int = 10,
  maxTipUsage: Int = 2,
  maxCheckpointFormationThreshold: Int = 25,
  maxTXInBlock: Int = 50,
  maxMessagesInBlock: Int = 1,
  peerInfoTimeout: Int = 3,
  randomTransactionLoopTimeSeconds: Int = 2,
  snapshotTriggeringTimeSeconds: Int = 2,
  formUndersizedCheckpointAfterSeconds: Int = 30,
  numFacilitatorPeers: Int = 2,
  maxTransactionsPerRound: Int = 3,
  emptyTransactionsRounds: Int = 5,
  amountTransactionsRounds: Int = 5,
  metricCheckInterval: Int = 10,
  maxMemPoolSize: Int = 35000,
  minPeerTimeAddedSeconds: Int = 30,
  maxActiveTipsAllowedInMemory: Int = 1000,
  maxAcceptedCBHashesInMemory: Int = 5000,
  peerHealthCheckInterval: Int = 30,
  peerDiscoveryInterval: Int = 60,
  snapshotHeightInterval: Int = 2,
  snapshotHeightDelayInterval: Int = 2,
  snapshotHeightRedownloadDelayInterval: Int = 4,
  snapshotInterval: Int = 10,
  formCheckpointTimeout: Int = 60,
  maxFaucetSize: Int = 1000,
  roundsPerMessage: Int = 10,
  recentSnapshotNumber: Int = 12,
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
    with MetricTimerDirective {

  import dao._

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("api-dispatcher")

  val config: Config = ConfigFactory.load()

  private val authEnabled = config.getBoolean("auth.enabled")
  private val authId: String = config.getString("auth.id")
  private var authPassword: String = config.getString("auth.password")

  val getEndpoints: Route =
    extractClientIP { clientIP =>
      get {
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
                  val cb = ccd.checkpointBlock.get

                  BlockUIOutput(
                    cb.soeHash,
                    ccd.height.get.min,
                    cb.parentSOEHashes,
                    cb.messages.map { _.signedMessageData.data.channelId }.distinct.map { channelId =>
                      ChannelValidationInfo(channelId, true)
                    }
                  )
                })
              }
          } ~
          path("messageService" / Segment) { channelId =>
            complete(dao.messageService.lookup(channelId).unsafeRunSync())
          } ~
          path("channelKeys") {
            complete(dao.channelService.toMap().unsafeRunSync().keys.toSeq)
          } ~
          path("channel" / "genesis" / Segment) { channelId =>
            complete(dao.channelService.lookup(channelId).unsafeRunSync())
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
                _.checkpointBlock.get.baseHash == blockHashForMessage
              }.head.checkpointBlock.get

              val messageProofInput = block.transactions.map { _.hash } ++ block.messages.map {
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

            onSuccess(proof.unsafeToFuture())(complete(_))
          } ~
          path("messages") {
            complete(dao.channelStorage.getLastNMessages(20))
          } ~
          path("messages" / Segment) { channelId =>
            complete(dao.channelStorage.getLastNMessages(20, Some(channelId)))
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

          }
      }
    }

  val postEndpoints =
    post {
      pathPrefix("channel") {
        path("open") {
          entity(as[ChannelOpen]) { request =>
            onComplete(
              ChannelMessage.createGenesis(request)
            ) { res =>
              complete(res.getOrElse(ChannelOpenResponse("Failed to open channel")))
            }
          }
        } ~
          path("send") {
            entity(as[ChannelSendRequest]) { send =>
              onComplete(ChannelMessage.createMessages(send)) { res =>
                complete(
                  res.getOrElse(ChannelSendResponse("Failed to create messages", Seq())).json
                )
              }
            }
          } ~
          path("send" / "json") {
            entity(as[ChannelSendRequestRawJson]) { send: ChannelSendRequestRawJson =>
              import constellation._
              val amended = ChannelSendRequest(
                send.channelId,
                send.messages.x[Seq[JValue]].map { _.json }
              )

              onComplete(ChannelMessage.createMessages(amended)) { res =>
                complete(res.getOrElse(ChannelOpenResponse("Failed to send raw json")).json)
              }
            }
          }
      } ~
        pathPrefix("peer") {
          path("remove") {
            entity(as[ChangePeerState]) { e =>
              onSuccess(dao.cluster.setNodeStatus(e.id, e.state).unsafeToFuture) {
                complete(StatusCodes.OK)
              }
            }
          } ~
            path("add") {
              entity(as[HostPort]) { hp =>
                onSuccess(dao.cluster.attemptRegisterPeer(hp).unsafeToFuture) { result =>
                  logger.debug(s"Add Peer Request: $hp. Result: $result")
                  complete(StatusCode.int2StatusCode(result.code))
                }
              }
            }
        } ~
        pathPrefix("download") {
          path("start") {
            Future { Download.download() }(ConstellationExecutionContext.edge)
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
        pathPrefix("password") {
          path("update") {
            entity(as[UpdatePassword]) { pu =>
              authPassword = pu.password
              complete(StatusCodes.OK)
            }
          }
        } ~
        path("ready") {
          def setNodeState =
            dao.cluster.isNodeReady
              .ifM(
                dao.cluster.setNodeState(NodeState.PendingDownload),
                dao.cluster.setNodeState(NodeState.Ready)
              )

          def broadcastNodeState =
            dao.cluster.getNodeState.flatMap { nodeState =>
              dao.cluster.broadcast(_.postNonBlockingIOUnit("status", SetNodeStatus(dao.id, nodeState)))
            }.flatTap {
              _.filter(_._2.isLeft).toList.traverse {
                case (id, e) => IO.delay(logger.warn(s"Unable to propagate status to node ID: $id", e))
              }
            }

          onSuccess((setNodeState *> broadcastNodeState).unsafeToFuture) { _ =>
            complete(StatusCodes.OK)
          }
        } ~
        path("peerHealthCheck") {
          val resetTimeout = 1.second
          val breaker = new CircuitBreaker(system.scheduler, maxFailures = 1, callTimeout = 5.seconds, resetTimeout)

          val response = dao.cluster.broadcast(_.getStringIO("health"))

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
            entity(as[Set[Id]]) { ids =>
              complete(Genesis.createGenesisAndInitialDistribution(dao.selfAddressStr, ids, dao.keyPair))
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
        path("send") {
          entity(as[SendToAddress]) { sendRequest =>
            logger.info(s"send transaction to address $sendRequest")

            val tx = createTransaction(
              dao.selfAddressStr,
              sendRequest.dst,
              sendRequest.amountActual,
              dao.keyPair,
              normalized = false
            )

            dao.transactionService
              .put(TransactionCacheData(tx))
              .unsafeRunSync()

            complete(tx.hash)
          }
        } ~
        path("addPeer") {
          entity(as[PeerMetadata]) { pm =>
            // TODO: check if it can be handled in onSuccess
            dao.cluster.addPeerMetadata(pm).unsafeToFuture

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

  private val mainRoutes: Route = cors() {
    decodeRequest {
      encodeResponse {
        getEndpoints ~ postEndpoints ~ configEndpoints ~ jsRequest ~ imageRoute ~ commonEndpoints ~ serveMainPage
      }
    }
  }

  private def myUserPassAuthenticator(credentials: Credentials): Option[String] =
    credentials match {
      case p @ Credentials.Provided(id) if id == authId && p.verify(authPassword) =>
        Some(id)
      case _ => None
    }

  val routes = withTimer("api") {
    if (authEnabled) {
      noAuthRoutes ~ authenticateBasic(realm = "secure site", myUserPassAuthenticator) { user =>
        mainRoutes
      }
    } else {
      noAuthRoutes ~ mainRoutes
    }
  }
}
