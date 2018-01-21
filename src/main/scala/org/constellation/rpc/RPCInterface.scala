package org.constellation.rpc

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.blockchain.{Block, GenesisBlock, Transaction}
import ProtocolInterface.{QueryAll, QueryLatest, ResponseBlock, ResponseBlockChain}
import akka.http.scaladsl.server.Route
import org.constellation.blockchain.Consensus.MineBlock
import org.constellation.p2p.PeerToPeer._
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats, native}

import scala.concurrent.{ExecutionContext, Future}

trait RPCInterface extends Json4sSupport {

  val blockChainActor: ActorRef
  val logger: Logger

  implicit val serialization: Serialization.type = native.Serialization
  implicit val stringUnmarshallers: FromEntityUnmarshaller[String] = PredefinedFromEntityUnmarshallers.stringUnmarshaller

  implicit def json4sFormats: Formats = DefaultFormats

  implicit val executionContext: ExecutionContext

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  val routes: Route =
    get {
      path("blocks") {
        val chain: Future[Seq[Block]] = (blockChainActor ? QueryAll).map {
          //This is a bit of a hack, since JSON4S doesn't serialize case objects well
          case ResponseBlockChain(blockChain) => blockChain.blocks.slice(0, blockChain.blocks.length -1) :+ GenesisBlock.copy()
        }
        complete(chain)
      }~
      path("peers") {
        complete( (blockChainActor ? GetPeers).mapTo[Peers] )
      }~
        path("id") {
          complete( (blockChainActor ? GetId).mapTo[Id] )
        }~
      path("latestBlock") {
        complete( (blockChainActor ? QueryLatest).map {
          case ResponseBlock(GenesisBlock) => GenesisBlock.copy()
          case ResponseBlock(block) => block
        })
      }
    }~
    post {
     path("mineBlock") {
       entity(as[Transaction]) { data =>
            logger.info(s"Got request to add new block $data")
            complete((blockChainActor ? data).mapTo[ResponseBlock].map {
              case ResponseBlock(block) => block
            })
       }
    }~
    path("addPeer") {
      entity(as[String]) { peerAddress =>
        logger.info(s"Got request to add new peer $peerAddress")
        blockChainActor ! AddPeer(peerAddress)
        complete(s"Added peer $peerAddress")
      }
    }
  }
}
