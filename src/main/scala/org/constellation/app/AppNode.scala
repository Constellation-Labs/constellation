package org.constellation.app

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.constellation.actor.Node
import org.constellation.rpc.RPCInterface

import scala.concurrent.ExecutionContextExecutor


/**
  * Main entry point for running a node.
  * This is the main initialization class for starting a Constellation node in practice.
  * Use AppNode builder for constructing test nodes within same JVM.
  * Otherwise rely on the BlockChainApp main method for regular use.
  */
class AppNode(
               val id: String,
               val httpInterface: String,
               val httpPort: Int,
               val actorNamePrefix: String = "constellation",
               timeoutSeconds: Int = 5,
               appendActorIdToName: Boolean = false
             )(
               implicit val system: ActorSystem,
               implicit val materialize: ActorMaterializer,
               implicit val executionContext: ExecutionContextExecutor
             ) extends RPCInterface {

  private val appendId: String = if (appendActorIdToName) "_" + id else ""
  val actorName: String = actorNamePrefix + appendId
  val blockChainActor: ActorRef = system.actorOf(Node.props(id), id)
  val logger = Logger("WebServer" + appendId)

  override implicit val timeout: Timeout = Timeout(timeoutSeconds, TimeUnit.SECONDS)

  Http().bindAndHandle(routes, httpInterface, httpPort)

}

object AppNode {

  def apply()(
    implicit system: ActorSystem,
    materialize: ActorMaterializer,
    executionContext: ExecutionContextExecutor
  ): AppNode = {
    val randomPort = scala.util.Random.nextInt(50000) + 5000
    new AppNode(s"ID_$randomPort", "127.0.0.1", randomPort, appendActorIdToName = true)
  }

}
