package org.constellation

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.{ActorRef, ActorSystem}
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
import org.constellation.crypto.Wallet
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, _}
import org.constellation.util.ServeUI
import org.json4s.native
import org.json4s.native.Serialization
import scalaj.http.HttpResponse

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

case class AddPeerRequest(host: String, udpPort: Int, httpPort: Int, id: Id)

class API(udpAddress: InetSocketAddress,
          val data: Data = null,
          cellManager: ActorRef)(implicit system: ActorSystem, val timeout: Timeout)
  extends Json4sSupport
    with Wallet
    with ServeUI {

  import data._

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("api-dispatcher")

  val logger = Logger(s"APIInterface")

  val config: Config = ConfigFactory.load()

  val authId: String = config.getString("auth.id")
  val authPassword: String = config.getString("auth.password")

  val getEndpoints: Route =
    extractClientIP { clientIP =>
      get {
          path("restart") {
            data.restartNode()
            complete(StatusCodes.OK)
          } ~
          path("setKeyPair") {
            parameter('keyPair) { kpp =>
              logger.debug("Set key pair " + kpp)
              val res = if (kpp.length > 10) {
                val rr = Try {
                  data.updateKeyPair(kpp.x[KeyPair])
                  StatusCodes.OK
                }.getOrElse(StatusCodes.BadRequest)
                rr
              } else StatusCodes.BadRequest
              complete(res)
            }
          } ~
          path("metrics") {
            val response = MetricsResult(
              (data.metricsManager ? GetMetrics).mapTo[Map[String, String]].getOpt().getOrElse(Map())
            )
            complete(response)
          } ~
          path("makeKeyPair") {
            val pair = constellation.makeKeyPair()
            wallet :+= pair
            complete(pair)
          } ~
          path("stackSize" / IntNumber) { num =>
            minGenesisDistrSize = num
            complete(StatusCodes.OK)
          } ~
          path("makeKeyPairs" / IntNumber) { numPairs =>
            val pair = Seq.fill(numPairs) {
              constellation.makeKeyPair()
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
          path("id") { // TODO : This should be served both on internal / external API
            // ^ Create shared routes for common functionality like this.
            complete(id)
          } ~
          path("nodeKeyPair") {
            complete(keyPair)
          } ~
          // TODO: revisit
          path("health") {
            complete(StatusCodes.OK)
          } ~
          path("peers") {
            val res = (data.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].getOpt().getOrElse(Map())
            complete(res)
          } ~
          path("peerids") {
            complete(peers.map {
              _.data
            })
          } ~
          path("hasGenesis") {
            if (data.genesisObservation.isDefined) {
              complete(StatusCodes.OK)
            } else {
              complete(StatusCodes.NotFound)
            }
          } ~
          path("checkpointTips") {
            complete(data.confirmedCheckpoints)
          } ~
          jsRequest ~
          serveMainPage
      }
    }

  private val postEndpoints =
    post {
      path("random") { // Temporary
        data.generateRandomTX = true
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
            Future {
              acceptGenesis(go)
              // TODO: Report errors and add validity check
            }
            complete(StatusCodes.OK)
          }
        }
      } ~
      path("sendTransactionToAddress"){
        entity(as[SendToAddress]) { e =>

          println(s"send transaction to address $e")

          Future {
            transactions.TransactionManager.handleSendToAddress(e, data)
          }

          complete(StatusCodes.OK)
        }
      } ~
      path("addPeer"){
        entity(as[AddPeerRequest]) { e =>

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
                new InetSocketAddress(ip, port.toInt)
              case a @ _ => {
                logger.debug(s"Unmatched Array: $a")
                throw new RuntimeException(s"Bad Match: $a")
              }
            }
          logger.debug(s"Set external IP RPC request $externalIp $addr")
          data.externalAddress = Some(addr)
          if (ipp.nonEmpty)
            data.apiAddress = Some(new InetSocketAddress(ipp, 9000))
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


  private val routes: Route = cors() {
    getEndpoints ~ postEndpoints ~ jsRequest ~ serveMainPage
  }

  def myUserPassAuthenticator(credentials: Credentials): Option[String] = {
    credentials match {
      case p @ Credentials.Provided(id)
        if id == authId && p.verify(authPassword) =>
        Some(id)
      case _ => None
    }
  }

  val authRoutes: Route = faviconRoute ~ routes

}
