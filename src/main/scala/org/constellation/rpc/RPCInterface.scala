package org.constellation.rpc

import java.security.PublicKey

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
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.constellation.consensus.Consensus.Initialize
import org.constellation.primitives.Transaction
import org.constellation.state.ChainStateManager.{CurrentChainStateUpdated, GetCurrentChainState}
import org.json4s.native.Serialization

import scala.concurrent.ExecutionContext

class RPCInterface(chainStateActor: ActorRef, peerToPeerActor: ActorRef, memPoolManagerActor: ActorRef, consensusActor: ActorRef)
                  (implicit executionContext: ExecutionContext, timeout: Timeout) extends Json4sSupport {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  implicit def json4sFormats: Formats = constellation.constellationFormats

  val logger = Logger(s"RPCInterface")

  case class Response(success: Boolean = true)

  val routes: Route =
    get {
      // TODO: revisit
      path("health") {
        complete(StatusCodes.OK)
      } ~
      path("blocks") {
        complete((chainStateActor ? GetCurrentChainState).mapTo[CurrentChainStateUpdated].map(f => f.chain))
      } ~
      path("peers") {
        complete((peerToPeerActor ? GetPeers).mapTo[Peers])
      } ~
      path("id") {
        complete((peerToPeerActor ? GetId).mapTo[Id])
      } ~ path("actorPath") {
        complete(peerToPeerActor.path.toSerializationFormat)
      } ~
      path("balance") {
        entity(as[PublicKey]) { account =>
          logger.debug(s"Received request to query account $account balance")

          // TODO: update balance
          // complete((chainStateActor ? GetBalance(account)).mapTo[Balance])

          complete(Response)
        }
      }
    } ~
    post {
      path("transaction") {
        entity(as[Transaction]) { transaction =>
          logger.debug(s"Received request to submit a new transaction $transaction")

          memPoolManagerActor ! transaction

          complete(transaction)
        }
      } ~
      path("peer") {
        entity(as[String]) { peerAddress =>
          logger.debug(s"Received request to add a new peer $peerAddress")

          peerToPeerActor ! AddPeer(peerAddress.replace("\"", ""))

          complete(StatusCodes.Created)
        }
      } ~
      // TODO: revist
      path("initialize") {

        consensusActor ! Initialize()

        complete(StatusCodes.Created)
      }
    }
}