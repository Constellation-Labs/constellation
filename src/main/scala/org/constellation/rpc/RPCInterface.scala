package org.constellation.rpc

import java.security.PublicKey
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.p2p.PeerToPeer._
import org.json4s.{Formats, native}
import akka.http.scaladsl.marshalling.Marshaller._
import org.constellation.primitives.Transaction.Transaction
import org.json4s.native.Serialization

import scala.concurrent.ExecutionContext

trait RPCInterface extends Json4sSupport {

  val chainStateActor: ActorRef
  val peerToPeerActor: ActorRef
  val memPoolManagerActor: ActorRef

  val logger: Logger

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  implicit def json4sFormats: Formats = constellation.constellationFormats

  implicit val executionContext: ExecutionContext

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  val routes: Route =
    get {
      path("blocks") {
        // TODO: update
      //  complete((chainStateActor ? GetChain).mapTo[FullChain])
        complete("")
      } ~
      path("peers") {

        complete((peerToPeerActor ? GetPeers).mapTo[Peers])
      } ~
      path("id") {
        complete((peerToPeerActor ? GetId).mapTo[Id])
      }
    } ~
    post {
      path("sendTx") {
        entity(as[Transaction]) { transaction =>
          logger.info(s"Got request to submit new transaction $transaction")
          memPoolManagerActor ! transaction
          // Had to remove the ask because otherwise it can create a loop with other nodes
          complete(transaction) // (blockChainActor ? transaction).mapTo[Transaction]
        }
      } ~
      path("addPeer") {
        entity(as[String]) { peerAddress =>
          logger.info(s"Got request to add new peer $peerAddress")
          complete((peerToPeerActor ? AddPeer(peerAddress)).mapTo[String])
        }
      } ~
      path("getBalance") {
        entity(as[PublicKey]) { account =>
          logger.info(s"Got request query account $account balance")
          // TODO: update balance
         // complete((chainStateActor ? GetBalance(account)).mapTo[Balance])
          complete("")
        }
      }
    }
}