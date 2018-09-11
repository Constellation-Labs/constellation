package org.constellation.p2p

import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.CustomDirectives.BannedIPEnforcer
import org.constellation.Data
import org.constellation.LevelDB.DBGet
import org.constellation.consensus.{EdgeProcessor, Validation}
import org.constellation.primitives.Schema._
import org.constellation.primitives.{IncrementMetric, PendingRegistration}
import org.constellation.util.CommonEndpoints
import org.json4s.native
import org.json4s.native.Serialization

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

case class PeerAuthSignRequest(salt: Long = Random.nextLong())
case class PeerRegistrationRequest(host: String, port: Int, key: String)

class PeerAPI(val dao: Data)(implicit val timeout: Timeout)
  extends Json4sSupport with CommonEndpoints with BannedIPEnforcer {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  private val logger = Logger(s"PeerAPI")

  private val config: Config = ConfigFactory.load()

  private var pendingRegistrations = Map[String, PeerRegistrationRequest]()

  private def getHostAndPortFromRemoteAddress(clientIP: RemoteAddress) = {
    clientIP.toIP.map{z => PeerIPData(z.ip.getCanonicalHostName, z.port)}
  }

  private val getEndpoints = {
    get {
      extractClientIP { clientIP =>
        path("ip") {
          complete(clientIP.toIP.map{z => PeerIPData(z.ip.getCanonicalHostName, z.port)})
        }
      }
    }
  }

  private val signEndpoints =
    post {
      path ("sign") {
        entity(as[PeerAuthSignRequest]) { e =>
          complete(hashSign(e.salt.toString, dao.keyPair))
        }
      } ~
      path("register") {
        extractClientIP { clientIP =>
          entity(as[PeerRegistrationRequest]) { request =>
            val maybeData = getHostAndPortFromRemoteAddress(clientIP)
            maybeData match {
              case Some(PeerIPData(host, portOption)) =>
                dao.peerManager ! PendingRegistration(host, request)
                pendingRegistrations = pendingRegistrations.updated(host, request)
                complete(StatusCodes.OK)
              case None =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      }
    }

  private val postEndpoints = {
    post {
      path ("deregister") {
        entity(as[PeerAuthSignRequest]) { e =>
          complete(hashSign(e.salt.toString, dao.keyPair))
        }
      }
    }
  }

  private val mixedEndpoints = {
    path("transaction" / Segment) { s =>
      put {
        entity(as[Transaction]) {
          tx =>
            Future {
              dao.metricsManager ! IncrementMetric("transactionMessagesReceived")
              Validation.validateTransaction(dao.dbActor, tx)
                .foreach{EdgeProcessor.handleTransaction(_, tx, dao)}
            }
            complete(StatusCodes.OK)
        }
      } ~
      get {
        val memPoolPresence = dao.transactionMemPool.get(s)
        val response = memPoolPresence.map { t =>
          TransactionQueryResponse(s, Some(t), inMemPool = true, inDAG = false, None)
        }.getOrElse{
          (dao.dbActor ? DBGet(s)).mapTo[Option[TransactionCacheData]].get().map{
            cd =>
              TransactionQueryResponse(s, Some(cd.transaction), memPoolPresence.nonEmpty, cd.inDAG, cd.cbEdgeHash)
          }.getOrElse{
            TransactionQueryResponse(s, None, inMemPool = false, inDAG = false, None)
          }
        }

        complete(response)
      } ~ complete (StatusCodes.BadRequest)
    } ~
    path("checkpoint" / Segment) { s =>
      put {
        entity(as[CheckpointBlock]) {
          cb =>
            Future{
              EdgeProcessor.handleCheckpoint(cb, dao)
            }
            complete(StatusCodes.OK)
        }
      } ~
      get {
        complete("CB goes here")
      } ~ complete (StatusCodes.BadRequest)
    }
  }

  val routes: Route = {
    rejectBannedIP {
      signEndpoints ~ enforceKnownIP {
        getEndpoints ~ postEndpoints ~ mixedEndpoints ~ commonEndpoints
      }
    }
  }

}
