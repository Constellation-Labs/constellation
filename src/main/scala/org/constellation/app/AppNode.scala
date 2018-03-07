package org.constellation.app

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.constellation.actor.Node
import org.constellation.p2p.PeerToPeer.AddPeer
import org.constellation.rpc.RPCInterface

import scala.concurrent.ExecutionContextExecutor
class AppNode(
               val id: String,
               val seedHost: String,
               val httpInterface: String,
               val httpPort: Int,
               timeoutSeconds: Int = 5,
               appendActorIdToName: Boolean = false
             )(
               implicit val system: ActorSystem,
               implicit val materialize: ActorMaterializer,
               implicit val executionContext: ExecutionContextExecutor
) extends RPCInterface {

  val actorName: String = "blockChainActor" + {if (appendActorIdToName) "_" + id else ""}
  val blockChainActor = system.actorOf(Node.props(id), id)
  val logger = Logger("AppNode_" + id)

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
    new AppNode(s"ID_$randomPort", seedHost, "0.0.0.0", randomPort, appendActorIdToName = true)
  }

}
