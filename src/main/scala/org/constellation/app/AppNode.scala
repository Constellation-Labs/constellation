package org.constellation.app

import java.security.PublicKey
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.constellation.actor.Node
import org.constellation.p2p.PeerToPeer.AddPeer
import org.constellation.rpc.RPCInterface
import org.constellation.wallet.KeyUtils

import scala.concurrent.ExecutionContextExecutor
class AppNode(
               val pubKey: PublicKey,
               val seedHost: String,
               val httpInterface: String,
               val httpPort: Int,
               timeoutSeconds: Int = 5,
               actorNameSuffix: String = ""
             )(
               implicit val system: ActorSystem,
               implicit val materialize: ActorMaterializer,
               implicit val executionContext: ExecutionContextExecutor
) extends RPCInterface {

  val actorName: String = "blockChainActor" + actorNameSuffix
  val blockChainActor: ActorRef = system.actorOf(Node.props(pubKey), actorName)
  val logger = Logger("AppNode_" + actorNameSuffix)

  override implicit val timeout: Timeout = Timeout(timeoutSeconds, TimeUnit.SECONDS)

  if (seedHost.nonEmpty) {
    logger.info(s"Attempting to connect to seed-host $seedHost")
    blockChainActor ! AddPeer(seedHost)
  } else {
    logger.info("No seed host configured, waiting for messages.")
  }

  Http().bindAndHandle(routes, httpInterface, httpPort)

}

object AppNode {

  def apply(seedHost: String = "")(
    implicit system: ActorSystem,
    materialize: ActorMaterializer,
    executionContext: ExecutionContextExecutor
  ): AppNode = {
    val randomPort = scala.util.Random.nextInt(50000) + 5000
    val pubKey = KeyUtils.makeKeyPair().getPublic
    new AppNode(pubKey, seedHost, "0.0.0.0", randomPort, actorNameSuffix = "ID_" + randomPort)
  }

}
