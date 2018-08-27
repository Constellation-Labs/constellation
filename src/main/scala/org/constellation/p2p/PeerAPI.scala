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
import org.constellation.primitives._
import org.constellation.util.HashSignature
import org.json4s.native
import org.json4s.native.Serialization
import akka.pattern.ask
import org.constellation.LevelDB.DBPut
import org.constellation.{AddPeerRequest, Data}
import org.constellation.consensus.Consensus.{CheckpointVote, InitializeConsensusRound, RoundHash}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Random, Try}

case class PeerAuthSignRequest(salt: Long = Random.nextLong())

class PeerAPI(
               dbActor: ActorRef,
               nodeManager: ActorRef,
               keyManager: ActorRef,
               metricsManager: ActorRef,
               peerManager: ActorRef,
               consensus: ActorRef,
               dao: Data
             )(implicit executionContext: ExecutionContext, val timeout: Timeout, val keyPair: KeyPair)
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
          val response = (peerManager ? APIBroadcast(_.get("health"))).mapTo[Map[Id, Future[scalaj.http.HttpResponse[String]]]]
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
          val askRes = keyManager ? SignRequest(e.salt.toString)
          askRes.mapTo[HashSignature].getOpt().map{
            complete(_)
          }.getOrElse(complete(StatusCodes.InternalServerError))
        }
      } ~
      path("addPeer"){
        entity(as[AddPeerRequest]) { e =>
          peerManager ! e
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
                dao.externalHostString = ip
                new InetSocketAddress(ip, port.toInt)
              case a @ _ => {
                logger.debug(s"Unmatched Array: $a")
                throw new RuntimeException(s"Bad Match: $a")
              }
            }
          logger.debug(s"Set external IP RPC request $externalIp $addr")
          dao.externalAddress = Some(addr)
          if (ipp.nonEmpty)
            dao.apiAddress = Some(new InetSocketAddress(ipp, 9000))
          complete(StatusCodes.OK)
        }
      } ~
      path("genesisObservation") {
        entity(as[Set[Id]]) { ids =>
          complete(dao.createGenesisAndInitialDistributionOE(ids))
        }
      } ~
      path("acceptGenesis") {
        entity(as[GenesisObservation]) { go =>

            // Store hashes for the edges
            go.genesis.store(dbActor)
            go.initialDistribution.store(dbActor)

            // Store the balance for the genesis TX minus the distribution along with starting rep score.
            go.genesis.resolvedTX.foreach{
              rtx =>
                dbActor ! DBPut(
                  rtx.dst.hash,
                  AddressCacheData(rtx.amount - go.initialDistribution.resolvedTX.map{_.amount}.sum, Some(1000D))
                )
            }

            // Store the balance for the initial distribution addresses along with starting rep score.
            go.initialDistribution.resolvedTX.foreach{ t =>
              dbActor ! DBPut(t.dst.hash, AddressCacheData(t.amount, Some(1000D)))
            }

            val numTX = (1 + go.initialDistribution.resolvedTX.size).toString
            metricsManager ! UpdateMetric("validTransactions", numTX)
            metricsManager ! UpdateMetric("uniqueAddressesInLedger", numTX)

            dao.genesisObservation = Some(go)
            dao.activeTips = Seq(
              TypedEdgeHash(go.genesis.resolvedCB.edge.signedObservationEdge.hash, EdgeHashType.CheckpointHash),
              TypedEdgeHash(go.initialDistribution.resolvedCB.edge.signedObservationEdge.hash, EdgeHashType.CheckpointHash)
            )

            metricsManager ! UpdateMetric("activeTips", "2")

          // TODO: Report errors and add validity check
          complete(StatusCodes.OK)
        }
      } ~
      path("startRandomTX") {
        entity(as[GenesisObservation]) { go =>
          dao.sendRandomTXV2 = !dao.sendRandomTXV2
          complete(dao.sendRandomTXV2.toString)
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
                    peerManager ! APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx))
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

                  // Trigger check if we should emit a CB

                  val fut = (peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]]
                  val peers = Try {
                    Await.result(fut, timeout.duration)
                  }.toOption

                  // TODO: wip
                  /*
                  consensus ! InitializeConsensusRound(peers.get.keySet, RoundHash("test"), (result) => {
                    assert(true)
                  }, CheckpointVote(EdgeService.createCheckpointEdge(dao.activeTips, dao.memPool.toSeq)) )
                  */

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
