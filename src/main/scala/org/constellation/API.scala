package org.constellation

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorSystem
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{entity, path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.Credentials
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.pattern.ask
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.crypto.{KeyUtils, SimpleWalletLike}
import org.constellation.p2p.Download
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, _}
import org.constellation.util.{CommonEndpoints, ServeUI}
import org.json4s.native
import org.json4s.native.Serialization
import scalaj.http.HttpResponse

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

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

case class ProcessingConfig(
                             maxWidth: Int = 10,
                             minCheckpointFormationThreshold: Int = 100,
                             numFacilitatorPeers: Int = 2,
                             randomTXPerRoundPerPeer: Int = 100,
                             metricCheckInterval: Int = 60,
                             maxMemPoolSize: Int = 2000,
                             minPeerTimeAddedSeconds: Int = 30,
                             maxActiveTipsAllowedInMemory: Int = 1000,
                             maxAcceptedCBHashesInMemory: Int = 5000,
                             peerHealthCheckInterval : Int = 30,
                             peerDiscoveryInterval : Int = 60,
                             snapshotHeightInterval: Int = 5,
                             snapshotHeightDelayInterval: Int = 15,
                             snapshotInterval: Int = 25,
                             checkpointLRUMaxSize: Int = 4000,
                             transactionLRUMaxSize: Int = 10000,
                             addressLRUMaxSize: Int = 10000,
                             formCheckpointTimeout: Int = 60,
                             maxFaucetSize: Int = 1000
) {

}


class API(udpAddress: InetSocketAddress)(implicit system: ActorSystem, val timeout: Timeout, val dao: DAO)
  extends Json4sSupport
    with SimpleWalletLike
    with ServeUI
    with CommonEndpoints {

  import dao._

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("api-dispatcher")

  val logger = Logger(s"APIInterface")

  val config: Config = ConfigFactory.load()

  private val authEnabled = config.getBoolean("auth.enabled")
  private val authId: String = config.getString("auth.id")
  private val authPassword: String = config.getString("auth.password")

  val getEndpoints: Route =
    extractClientIP { clientIP =>
      get {
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
          entity(as[HostPort]) { case hp @ HostPort(host, port) =>
            onComplete{
              PeerManager.attemptRegisterPeer(hp)
            } { result =>

              complete(StatusCodes.OK)
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
        val response = (peerManager ? APIBroadcast(_.get("health"))).mapTo[Map[Id, Future[HttpResponse[String]]]]
        val res = response.getOpt().map{
          idMap =>

            val res = idMap.map{
              case (id, fut) =>
                val resp = fut.get()
                id -> resp.isSuccess
//                val maybeResponse = fut.getOpt()
//                id -> maybeResponse.exists{_.isSuccess}
            }.toSeq

            complete(res)

        }.getOrElse(complete(StatusCodes.InternalServerError))

        res
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
                import better.files._
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
        getEndpoints ~ decodeRequest(postEndpoints) ~ jsRequest ~ commonEndpoints ~ serveMainPage
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

  val routes =
    if (authEnabled) {
      noAuthRoutes ~ authenticateBasic(realm = "secure site",
        myUserPassAuthenticator) {
        user =>
          mainRoutes
      }
    } else {
      noAuthRoutes ~ mainRoutes
    }

}
