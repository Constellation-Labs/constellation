package org.constellation.p2p

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
import org.constellation.primitives.Schema.{PeerIPData, Transaction}
import org.constellation.primitives.{APIBroadcast, IncrementMetric, SignRequest, TransactionValidation}
import org.constellation.util.HashSignature
import org.json4s.native
import org.json4s.native.Serialization
import akka.pattern.ask
import org.constellation.Data

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random


case class PeerAuthSignRequest(salt: Long = Random.nextLong())

class PeerAPI(
               dbActor: ActorRef,
               nodeManager: ActorRef,
               keyManager: ActorRef,
               metricsManager: ActorRef,
               peerManager: ActorRef,
               dao: Data
             )(implicit executionContext: ExecutionContext, val timeout: Timeout)
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
          } /*~
        path("info") {
          complete()
        }
      }*/
      }
    }
  }

  private val postEndpoints =
    post {
      path ("sign") {
        entity(as[PeerAuthSignRequest]) { e =>
          val askRes = keyManager ? SignRequest(e.salt.toString)
          askRes.mapTo[HashSignature].getOpt().map{
            complete(_)
          }.getOrElse(complete(StatusCodes.InternalServerError))
        }
      }
    }

  private val mixedEndpoints = {
    path("transaction" / Segment) { s =>
      put {
        entity(as[Transaction]) {
          tx =>
            Future{
              // Validate transaction TODO : This can be more efficient, calls get repeated several times
              // in event where a new signature is being made by another peer it's most likely still valid, should
              // cache the results of this somewhere.
              TransactionValidation.validateTransaction(dbActor, tx).foreach{
                // TODO : Increment metrics here for each case
                case true =>

                  val txPrime = if (!tx.signatures.exists(_.publicKey == dao.keyPair.getPublic)) {
                    // We haven't yet signed this TX
                    val tx2 = tx.plus(dao.keyPair)
                    // Send peers new signature
                    peerManager ! APIBroadcast()
                    tx2
                  } else {
                    // We have already signed this transaction,
                    tx
                  }

                  // Add to memPool or update an existing hash with new signatures
                  if (dao.memPoolOE.contains(tx.hash)) {
                    dao.memPoolOE(tx.hash) = dao.memPoolOE(tx.hash).plus(txPrime)
                  } else {
                    dao.memPoolOE(tx.hash) = txPrime
                  }

                case false =>
                  metricsManager ! IncrementMetric("invalidTransactions")

              }
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
