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
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, IncrementMetric, SignRequest, TransactionValidation}
import org.constellation.util.HashSignature
import org.json4s.native
import org.json4s.native.Serialization
import akka.pattern.ask
import org.constellation.Data
import org.constellation.LevelDB.DBPut

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

  val minTXSignatureThreshold = 3
  val minCheckpointFormationThreshold = 3

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

                  // Check to see if we should add our signature to the transaction
                  val txPrime = if (!tx.signatures.exists(_.publicKey == dao.keyPair.getPublic)) {
                    // We haven't yet signed this TX
                    val tx2 = tx.plus(dao.keyPair)
                    // Send peers new signature
                    peerManager ! APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx2))
                    tx2
                  } else {
                    // We have already signed this transaction,
                    tx
                  }

                  // Add to memPool or update an existing hash with new signatures
                  if (dao.txMemPoolOE.contains(tx.hash)) {

                    // Merge signatures together
                    val updated = dao.txMemPoolOE(tx.hash).plus(txPrime)

                    // Check to see if we have enough signatures to include in CB
                    if (updated.signatures.size >= minTXSignatureThreshold) {

                      // Set threshold as met
                      dao.txMemPoolOEThresholdMet += tx.hash

                      if (dao.txMemPoolOEThresholdMet.size >= minCheckpointFormationThreshold) {

                        // Form new checkpoint block.

                        // TODO : Validate this batch doesn't have a double spend, if it does,
                        // just drop all conflicting.

                        val taken = dao.txMemPoolOEThresholdMet.take(minCheckpointFormationThreshold)
                        dao.txMemPoolOEThresholdMet --= taken
                        taken.foreach{dao.txMemPoolOE.remove}
                        val ced = CheckpointEdgeData(taken.toSeq.sorted)
/*
                        val oe = ObservationEdge(
                          TypedEdgeHash(CoinBaseHash, EdgeHashType.ValidationHash),
                          TypedEdgeHash(genTXHash, EdgeHashType.TransactionHash),
                          data = Some(TypedEdgeHash(cb.hash, EdgeHashType.CheckpointDataHash))
                        )

                        val soe = signedObservationEdge(oe)
*/


                      }
                    }
                    dao.txMemPoolOE(tx.hash) = updated
                  } else {
                    dao.txMemPoolOE(tx.hash) = txPrime
                  }

                  // Trigger check if we should emit a CB




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
