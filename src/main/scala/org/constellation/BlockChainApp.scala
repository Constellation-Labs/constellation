package org.constellation

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.constellation.actor.Node
import org.constellation.rpc.RPCInterface

import scala.concurrent.ExecutionContextExecutor

/**
  * This needs to be refactored to spin up one or two proceses, one for protocol and one (or zero) for consensus
  * the main method to this singleton needs to execute a hylomorphic method that takes types representing these actors
  * ex:
  * def hylo[F[_] : Functor, A, B](f: F[B] => B)(g: A => F[A]): A => B =
      a => f(g(a) map hylo(f)(g))
  */
object BlockChainApp extends App with RPCInterface {

  implicit val system: ActorSystem = ActorSystem("BlockChain")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val config = ConfigFactory.load()

  override implicit val timeout: Timeout = Timeout(
    config.getInt("blockchain.defaultRPCTimeoutSeconds"), TimeUnit.SECONDS)

  val logger = Logger("WebServer")

  val id = args.headOption.getOrElse("ID")
  val blockChainActor = system.actorOf(Node.props(id), "constellation")
  Http().bindAndHandle(routes, config.getString("http.interface"), config.getInt("http.port"))
}
