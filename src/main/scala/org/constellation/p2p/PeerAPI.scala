package org.constellation.p2p

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorRef
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, IncrementMetric, TransactionValidation}
import org.constellation.util.HashSignature
import org.json4s.native
import org.json4s.native.Serialization
import akka.pattern.ask
import org.constellation.Data
import org.constellation.LevelDB.DBPut
import org.constellation.consensus.TransactionProcessor
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

case class PeerAuthSignRequest(salt: Long = Random.nextLong())

class PeerAPI(dao: Data)(implicit executionContext: ExecutionContext, val timeout: Timeout)
  extends Json4sSupport {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  val logger = Logger(s"PeerAPI")

  val config: Config = ConfigFactory.load()

  private val getEndpoints = {
    extractClientIP { clientIP =>
      get {
        path("health") {
          complete(StatusCodes.OK)
        } ~
        path("ip") {
          complete(clientIP.toIP.map{z => PeerIPData(z.ip.getCanonicalHostName, z.port)})
        } ~
        path("peerHealthCheck") {
          val response = (dao.peerManager ? APIBroadcast(_.get("health"))).mapTo[Map[Id, Future[scalaj.http.HttpResponse[String]]]]
          val res = response.getOpt().map{
            idMap =>

              val res = idMap.map{
                case (id, fut) =>
                  val maybeResponse = fut.getOpt()
                  id -> maybeResponse.exists(f => f.isSuccess)
              }.toSeq

              complete(res)

          }.getOrElse(complete(StatusCodes.InternalServerError))

          res
        } ~
        path("id") {
          complete(dao.id)
        }
      }
    }
  }

  private val postEndpoints =
    post {
      path ("sign") {
        entity(as[PeerAuthSignRequest]) { e =>
          complete(hashSign(e.salt.toString, dao.keyPair))
        }
      }
    }

  private val mixedEndpoints = {
    path("transaction" / Segment) { s =>
      put {
        entity(as[Transaction]) {
          tx =>
            Future{
              TransactionProcessor.handleTransaction(tx, dao)(executionContext = executionContext, keyPair = dao.keyPair)
            }
            complete(StatusCodes.OK)
        }
      } ~
      get {
        complete("Transaction goes here")
      } ~ complete (StatusCodes.BadRequest)
    }
  }

  val routes: Route = {
    getEndpoints ~ postEndpoints ~ mixedEndpoints
  }

}
