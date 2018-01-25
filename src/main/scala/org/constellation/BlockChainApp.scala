package org.constellation


import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.constellation.app.AppNode


import scala.concurrent.ExecutionContextExecutor

/**
  * Main entry point for running a node.
  * Note: this can only be used to run one node per JVM. This is the main
  * initialization class for starting a Constellation node in practice.
  *
  * It initializes an ActorSystem (and you probably don't want multiple ActorSystems
  * running per JVM) Use AppNode builder for constructing test nodes within same JVM.
  */
object BlockChainApp extends App {

  val logger = Logger("AppInit")

  implicit val system: ActorSystem = ActorSystem("BlockChain")
  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val config = ConfigFactory.load()
  private val timeout = config.getInt("blockchain.defaultRPCTimeoutSeconds")
  private val seedHost = config.getString("blockchain.seedHost")
  private val id = args.headOption.getOrElse("ID")
  private val httpInterface = config.getString("http.interface")
  private val httpPort = config.getInt("http.port")

  logger.info(s"Initializing main BlackChainActor with id: $id and seedHost: $seedHost " +
  s"with timeout: $timeout on interface: $httpInterface port: $httpPort")

  val appNode = new AppNode(id, seedHost, httpInterface, httpPort, timeout)

}
