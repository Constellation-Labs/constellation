package org.constellation.p2p

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.{ActorRef, ActorSystem}
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
import org.constellation.primitives.{APIBroadcast, IncrementMetric}
import org.constellation.util.{CommonEndpoints, HashSignature}
import org.json4s.native
import org.json4s.native.Serialization
import akka.pattern.ask
import org.constellation.Data
import org.constellation.LevelDB.DBPut
import org.constellation.LevelDB.{DBGet, DBPut}
import org.constellation.consensus.Consensus.{ConsensusProposal, ConsensusVote, StartConsensusRound}
import org.constellation.consensus.EdgeProcessor.HandleTransaction
import org.constellation.consensus.{Consensus, EdgeProcessor}
import org.constellation.serializer.KryoSerializer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

case class PeerAuthSignRequest(salt: Long = Random.nextLong())

class PeerAPI(val dao: Data)(implicit system: ActorSystem, val timeout: Timeout)
  extends Json4sSupport with CommonEndpoints {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("api-dispatcher")

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  val logger = Logger(s"PeerAPI")

  val config: Config = ConfigFactory.load()

  private val getEndpoints = {
    extractClientIP { clientIP =>
      get {
        path("ip") {
          complete(clientIP.toIP.map{z => PeerIPData(z.ip.getCanonicalHostName, z.port)})
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
            Future{
              logger.debug(s"transaction endpoint, $tx")
              dao.edgeProcessor ! HandleTransaction(tx)
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
    getEndpoints ~ postEndpoints ~ mixedEndpoints ~ commonEndpoints
  }

}
