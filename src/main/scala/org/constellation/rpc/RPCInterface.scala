package org.constellation.rpc

import java.net.InetSocketAddress
import java.security.PublicKey

import akka.actor.ActorRef
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.consensus.Consensus.{DisableConsensus, EnableConsensus, GenerateGenesisBlock}
import org.constellation.p2p.PeerToPeer._
import org.constellation.primitives.{Block, BlockSerialized, Transaction}
import org.constellation.state.ChainStateManager.{CurrentChainStateUpdated, GetCurrentChainState}
import org.constellation.state.MemPoolManager.AddTransaction
import org.json4s.native.Serialization
import org.json4s.{Formats, native}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

class RPCInterface(
                    chainStateActor: ActorRef,
                    peerToPeerActor: ActorRef,
                    memPoolManagerActor: ActorRef,
                    consensusActor: ActorRef,
                    udpAddress: InetSocketAddress
                  )
                  (implicit executionContext: ExecutionContext, timeout: Timeout) extends Json4sSupport {

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  implicit def json4sFormats: Formats = constellation.constellationFormats

  val logger = Logger(s"RPCInterface")

  // TODO: temp until someone can show me how to create one of these custom serializers
  def serializeBlocks(blocks: Seq[Block]): Seq[BlockSerialized] = {
    blocks.map(b => {
      BlockSerialized(b.parentHash, b.height, b.signature,
        b.clusterParticipants, b.round, b.transactions)
    })
  }

  val routes: Route =
    get {
      // TODO: revisit
      path("health") {
        complete(StatusCodes.OK)
      } ~
      // TODO: revist
      path("generateGenesisBlock") {

        val future = consensusActor ? GenerateGenesisBlock(peerToPeerActor)

        val responseFuture: Future[Block] = future.mapTo[Block]

        val genesisBlock = Await.result(responseFuture, timeout.duration)

        complete(serializeBlocks(Seq(genesisBlock)).take(1))
      } ~
      path("enableConsensus") {

        consensusActor ! EnableConsensus()

        complete(StatusCodes.OK)
      } ~
      path("disableConsensus") {

        consensusActor ! DisableConsensus()

        complete(StatusCodes.OK)
      } ~
      path("blocks") {
        val future = chainStateActor ? GetCurrentChainState

        val responseFuture: Future[CurrentChainStateUpdated] = future.mapTo[CurrentChainStateUpdated]

        val chainStateUpdated = Await.result(responseFuture, timeout.duration)

        val chain = chainStateUpdated.chain

        val blocks = serializeBlocks(chain.chain)

        complete(blocks)
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

          complete(StatusCodes.OK)
        }
      }
    } ~
    post {
      path("transaction") {
        entity(as[Transaction]) { transaction =>
          logger.debug(s"Received request to submit a new transaction $transaction")

          memPoolManagerActor ! AddTransaction(transaction)

          complete(transaction)
        }
      } ~
      path("peer") {
        entity(as[String]) { peerAddress =>
          logger.debug(s"Received request to add a new peer $peerAddress")
          Try {
            peerAddress.replaceAll('"'.toString,"").split(":") match {
              case Array(ip, port) => new InetSocketAddress(ip, port.toInt)
              case a@_ => logger.debug(s"Unmatched Array: $a"); throw new RuntimeException(s"Bad Match: $a");
            }
          }.toOption match {
            case None =>
              complete(StatusCodes.BadRequest)
            case Some(v) =>
              val fut = (peerToPeerActor ?  AddPeerFromLocal(v)).mapTo[StatusCode]
              val res = Try{Await.result(fut, timeout.duration)}.toOption
              res match {
                case None =>
                  complete(StatusCodes.RequestTimeout)
                case Some(f) =>
                  complete(f)
              }
          }
        }
      } ~
      path("ip") {
        entity(as[String]) { externalIp =>
          val addr = externalIp.replaceAll('"'.toString,"").split(":") match {
            case Array(ip, port) => new InetSocketAddress(ip, port.toInt)
            case a@_ => { logger.debug(s"Unmatched Array: $a"); throw new RuntimeException(s"Bad Match: $a"); }
          }
          peerToPeerActor ! SetExternalAddress(addr)
          complete(StatusCodes.OK)
        }
      }
    }
}