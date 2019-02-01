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
import akka.pattern.{CircuitBreaker, ask}
import akka.util.Timeout
import better.files.{File, _}
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.softwaremill.sttp.Response
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.crypto.{KeyUtils, SimpleWalletLike}
import org.constellation.p2p.Download
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, _}
import org.constellation.serializer.KryoSerializer
import org.constellation.util.{CommonEndpoints, MerkleTree, ServeUI}
import org.json4s.native
import org.json4s.native.Serialization

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class PeerMetadata(
                     host: String,
                     udpPort: Int,
                     httpPort: Int,
                     id: Id,
                     nodeState: NodeState = NodeState.Ready,
                     timeAdded: Long = System.currentTimeMillis(),
                     auxHost: String = ""
                   )


case class HostPort(host: String, port: Int)
case class RemovePeerRequest(host: Option[HostPort] = None, id: Option[Id] = None)
case class UpdatePassword(password: String)

case class ProcessingConfig(
                             maxWidth: Int = 10,
                             minCheckpointFormationThreshold: Int = 50,
                             numFacilitatorPeers: Int = 2,
                             randomTXPerRoundPerPeer: Int = 100,
                             metricCheckInterval: Int = 60,
                             maxMemPoolSize: Int = 2000,
                             minPeerTimeAddedSeconds: Int = 30,
                             maxActiveTipsAllowedInMemory: Int = 1000,
                             maxAcceptedCBHashesInMemory: Int = 5000,
                             peerHealthCheckInterval : Int = 30,
                             peerDiscoveryInterval : Int = 60,
                             snapshotHeightInterval: Int = 2,
                             snapshotHeightDelayInterval: Int = 5,
                             snapshotInterval: Int = 25,
                             checkpointLRUMaxSize: Int = 4000,
                             transactionLRUMaxSize: Int = 10000,
                             addressLRUMaxSize: Int = 10000,
                             formCheckpointTimeout: Int = 60,
                             maxFaucetSize: Int = 1000,
                             roundsPerMessage: Int = 10
) {

}


class API(udpAddress: InetSocketAddress)(implicit system: ActorSystem, val timeout: Timeout, val dao: DAO)
  extends Json4sSupport
    with SimpleWalletLike
    with ServeUI
    with CommonEndpoints
    with StrictLogging {

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
        path ("channel" / Segment) { channelHash =>



          val res = Snapshot.findLatestMessageWithSnapshotHash(0, dao.messageService.get(channelHash))

          val proof = res.flatMap{ cmd =>

            cmd.snapshotHash.flatMap{ snapshotHash =>

              tryWithMetric({
                KryoSerializer.deserializeCast[StoredSnapshot](File(dao.snapshotPath, snapshotHash).byteArray)
              },
                "readSnapshotForMessage"
              ).toOption.map{ storedSnapshot =>

                val blockProof = MerkleTree(storedSnapshot.snapshot.checkpointBlocks.toList).createProof(cmd.blockHash.get)
                val block = storedSnapshot.checkpointCache.filter {
                  _.checkpointBlock.get.baseHash == cmd.blockHash.get
                }.head.checkpointBlock.get
                val messageProofInput = block.transactions.map{_.hash} ++ block.checkpoint.edge.resolvedObservationEdge.data.get.messages.map{_.signedMessageData.signatures.hash}
                val messageProof = MerkleTree(messageProofInput.toList).createProof(cmd.channelMessage.signedMessageData.signatures.hash)
                ChannelProof(
                  cmd,
                  blockProof,
                  messageProof
                )
              }


            }

          }

          complete(proof)
        } ~
          path("restart") { // TODO: Revisit / fix
            dao.restartNode()
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
          path("metrics") {
            val response = MetricsResult(
              (dao.metricsManager ? GetMetrics).mapTo[Map[String, String]].getOpt().getOrElse(Map())
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
          path("peerids") {
            complete(peers.map {
              _.data
            })
          } ~
          path("hasGenesis") {
            if (dao.genesisObservation.isDefined) {
              complete(StatusCodes.OK)
            } else {
              complete(StatusCodes.NotFound)
            }
          } ~
          path("dashboard") {
            val txs = dao.transactionService.getLast20TX
            val self = Node(selfAddress.address,
              selfPeer.data.externalAddress.map(_.getHostName).getOrElse(""),
              selfPeer.data.externalAddress.map(_.getPort).getOrElse(0))
            val peerMap = dao.peerInfo.toSeq.map {
              case (id,pd) => Node(id.address.address, pd.peerMetadata.host, pd.peerMetadata.httpPort)
            } :+ self

            complete(
              Map(
                "peers" -> peerMap,
                "transactions" -> txs
              ))

          }
      }
    }

  private val postEndpoints =
    post {
      pathPrefix("peer") {
        path("remove") {
          entity(as[RemovePeerRequest]) { e =>
            dao.peerManager ! e
            complete(StatusCodes.OK)
          }
        } ~
        path("add") {
          entity(as[HostPort]) { hp =>
            onSuccess(PeerManager.attemptRegisterPeer(hp)) { result =>
              logger.info(s"Add Peer Request: $hp. Result: $result")
              complete(StatusCode.int2StatusCode(result.code))
            }

          }
        }
      } ~
      pathPrefix("download") {
        path("start") {
          Future{Download.download()}(dao.edgeExecutionContext)
          complete(StatusCodes.OK)
        }
      } ~
      pathPrefix("config") {
        path("update") {
          entity(as[ProcessingConfig]) { cu =>
            dao.processingConfig = cu
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
      path("ready") { // Temp
        if (dao.nodeState != NodeState.Ready) {
          dao.nodeState = NodeState.Ready
        } else {
          dao.nodeState = NodeState.PendingDownload
        }
        dao.peerManager ! APIBroadcast(_.post("status", SetNodeStatus(dao.id, dao.nodeState)))
        dao.metricsManager ! UpdateMetric("nodeState", dao.nodeState.toString)
        complete(StatusCodes.OK)
      } ~
      path("random") { // Temporary
        dao.generateRandomTX = !dao.generateRandomTX
        dao.metricsManager ! UpdateMetric("generateRandomTX", dao.generateRandomTX.toString)
        complete(StatusCodes.OK)
      } ~
      path("peerHealthCheck") {
        val resetTimeout = 1.second
        val breaker = new CircuitBreaker(system.scheduler,
          maxFailures = 1,
          callTimeout = 5.seconds,
          resetTimeout
        )

        val response = (peerManager ? APIBroadcast(_.get("health"))).mapTo[Map[Id, Future[Response[String]]]]
        onCompleteWithBreaker(breaker)(response) {
          case Success(idMap) =>

            val res = idMap.map {
              case (id, fut) =>
                val resp = fut.get()
                id -> resp.isSuccess
              //                val maybeResponse = fut.getOpt()
              //                id -> maybeResponse.exists{_.isSuccess}
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
            complete(createGenesisAndInitialDistribution(ids))
          }
        } ~
        path("accept") {
          entity(as[GenesisObservation]) { go =>
            dao.acceptGenesis(go, setAsTips = true)
            // TODO: Report errors and add validity check
            complete(StatusCodes.OK)
          }
        }
      } ~
      path("send"){
        entity(as[SendToAddress]) { sendRequest =>

          logger.info(s"send transaction to address $sendRequest")

          val tx = createTransaction(dao.selfAddressStr, sendRequest.dst, sendRequest.amountActual, dao.keyPair, normalized = false)
          dao.threadSafeTXMemPool.put(tx, overrideLimit = true)

          complete(tx.hash)
        }
      } ~
      path("addPeer"){
        entity(as[PeerMetadata]) { e =>

          Future {
            peerManager ! e
          }

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
                externalHostString = ip
                file"external_host_ip".write(ip)
                new InetSocketAddress(ip, port.toInt)
              case a @ _ => {
                logger.debug(s"Unmatched Array: $a")
                throw new RuntimeException(s"Bad Match: $a")
              }
            }
          logger.debug(s"Set external IP RPC request $externalIp $addr")
          dao.externalAddress = Some(addr)
          dao.metricsManager ! UpdateMetric("externalHost", dao.externalHostString)
          if (ipp.nonEmpty)
            dao.apiAddress = Some(new InetSocketAddress(ipp, 9000))
          complete(StatusCodes.OK)
        }
      } ~
      path("reputation") {
        entity(as[Seq[UpdateReputation]]) { ur =>
          secretReputation = ur.flatMap { r =>
            r.secretReputation.map {
              id -> _
            }
          }.toMap
          publicReputation = ur.flatMap { r =>
            r.publicReputation.map {
              id -> _
            }
          }.toMap
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
        getFromResource("favicon.ico")
      }
    }

  private val mainRoutes: Route = cors() {
    decodeRequest {
      encodeResponse {
        getEndpoints ~ postEndpoints ~ jsRequest ~ commonEndpoints ~ serveMainPage
      }
    }
  }

  private def myUserPassAuthenticator(credentials: Credentials): Option[String] = {
    credentials match {
      case p @ Credentials.Provided(id)
        if id == authId && p.verify(authPassword) =>
        Some(id)
      case _ => None
    }
  }

  val routes = if (authEnabled) {
    noAuthRoutes ~ authenticateBasic(realm = "secure site",
      myUserPassAuthenticator) {
      user =>
        mainRoutes
    }
  } else {
    noAuthRoutes ~ mainRoutes
  }

}
