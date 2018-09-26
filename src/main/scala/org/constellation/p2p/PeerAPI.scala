package org.constellation.p2p

import akka.actor.ActorSystem
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
import org.constellation.LevelDB.DBGet
import org.constellation.consensus.Consensus.{ConsensusProposal, ConsensusVote, StartConsensusRound}
import org.constellation.consensus.EdgeProcessor.HandleTransaction
import org.constellation.consensus.{Consensus, EdgeProcessor}
import org.constellation.primitives.Schema._
import org.constellation.primitives.{Deregistration, IncrementMetric, PendingRegistration}
import org.constellation.serializer.KryoSerializer
import org.constellation.util.CommonEndpoints
import org.constellation.{ConstellationNode, Data}
import org.json4s.native
import org.json4s.native.Serialization

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

case class PeerAuthSignRequest(salt: Long = Random.nextLong())
case class PeerRegistrationRequest(host: String, port: Int, key: String)
case class PeerUnregister(host: String, port: Int, key: String)

class PeerAPI(node: ConstellationNode, val dao: Data)(implicit system: ActorSystem, val timeout: Timeout)
  extends Json4sSupport with CommonEndpoints with BannedIPEnforcer {

  override val parentNode = node

  implicit val serialization: Serialization.type = native.Serialization

  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("api-dispatcher")

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

  private val postEndpoints =
    post {
      path ("deregister") {
        extractClientIP { clientIP =>
          entity(as[PeerUnregister]) { request =>
            val maybeData = getHostAndPortFromRemoteAddress(clientIP)
            maybeData match {
              case Some(PeerIPData(host, portOption)) =>
                dao.peerManager ! Deregistration(request.host, request.port, request.key)
                complete(StatusCodes.OK)
              case None =>
                complete(StatusCodes.BadRequest)
            }
          }
      }
    } ~
      path("startConsensusRound") {
        entity(as[Array[Byte]]) { e =>
          Future {
            val message = KryoSerializer.deserialize(e).asInstanceOf[StartConsensusRound[Consensus.Checkpoint]]

            logger.debug(s"start consensus round endpoint, $message")

            dao.consensus ! ConsensusVote(message.id, message.data, message.roundHash)
          }

          complete(StatusCodes.OK)
        }
      } ~
      path("checkpointEdgeProposal") {
        entity(as[Array[Byte]]) { e =>
          Future {
            val message = KryoSerializer.deserialize(e).asInstanceOf[ConsensusProposal[Consensus.Checkpoint]]

            logger.debug(s"checkpoint edge proposal endpoint, $message")

            dao.consensus ! ConsensusProposal(message.id, message.data, message.roundHash)
          }

          complete(StatusCodes.OK)
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

              logger.debug(s"transaction endpoint, $tx")
              dao.edgeProcessor ! HandleTransaction(tx)
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
        complete("Transaction goes here")
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
