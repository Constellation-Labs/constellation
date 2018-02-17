package org.constellation.rpc

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.{PredefinedFromEntityUnmarshallers}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.blockchain.Consensus.PerformConsensus
import org.constellation.blockchain._
import org.constellation.p2p.PeerToPeer._
import org.constellation.rpc.ProtocolInterface._
import org.json4s.{DefaultFormats, Formats, native}
import akka.http.scaladsl.marshalling.Marshaller._

import scala.concurrent.ExecutionContext

trait RPCInterface extends Json4sSupport {

  val blockChainActor: ActorRef
  val logger: Logger

  implicit val serialization = native.Serialization
  implicit val stringUnmarshallers = PredefinedFromEntityUnmarshallers.stringUnmarshaller

  implicit def json4sFormats: Formats = DefaultFormats

  implicit val executionContext: ExecutionContext

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  val routes: Route =
    get {
      path("blocks") {
        complete((blockChainActor ? GetChain).mapTo[FullChain])
      } ~
        path("peers") {
          complete((blockChainActor ? GetPeers).mapTo[Peers])
        }~
        path("id") {
          complete((blockChainActor ? GetId).mapTo[Id])
        }~
        path("performConsensus") {
          complete((blockChainActor ? PerformConsensus).mapTo[String]) //TODO casting to string here as we need custom marshaling
        }
      }~
        post {
          path("sendTx") {
            entity(as[Transaction]) { transaction =>
              logger.info(s"Got request to submit new transaction $transaction")
              complete((blockChainActor ? transaction).mapTo[String])
            }
          }~
            path("addPeer") {
              entity(as[String]) { peerAddress =>
                logger.info(s"Got request to add new peer $peerAddress")
                blockChainActor ! AddPeer(peerAddress)
                complete(s"Added peer $peerAddress")
              }
            }~
            path("getBalance") {
              entity(as[String]) { account =>
                logger.info(s"Got request query account $account balance")
                complete((blockChainActor ? GetBalance(account)).mapTo[String])
              }
            }
        }
}
