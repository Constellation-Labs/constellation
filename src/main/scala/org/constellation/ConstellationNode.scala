package org.constellation

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.io.Udp
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.{CheckpointMemPoolVerifier, CheckpointUniqueSigner, Consensus, EdgeProcessor}
import org.constellation.crypto.KeyUtils
import org.constellation.p2p.{PeerAPI, UDPActor}
import org.constellation.primitives._
import org.constellation.util.{APIClient, Heartbeat}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object ConstellationNode {

  def main(args: Array[String]): Unit = {
    val logger = Logger(s"Main")
    logger.info("Main init")

    Try {


      implicit val system: ActorSystem = ActorSystem("Constellation")
      implicit val materializer: ActorMaterializer = ActorMaterializer()
      implicit val executionContext: ExecutionContext = system.dispatchers.lookup("main-dispatcher")

      logger.info("Config load")
      val config = ConfigFactory.load()

      val rpcTimeout = config.getInt("rpc.timeout")


      val seeds = Seq()
      val hostName = "127.0.0.1"
      val requestExternalAddressCheck = false
      /*
          val seeds = args.headOption.map(_.split(",").map{constellation.addressToSocket}.toSeq).getOrElse(Seq())

          val hostName = if (args.length > 1) {
            args(1)
          } else "127.0.0.1"

          val requestExternalAddressCheck = if (args.length > 2) {
            args(2).toBoolean
          } else false
      */

      // TODO: update to take from config
      logger.info("pre Key pair")
      val keyPair = KeyUtils.makeKeyPair()
      logger.info("post key pair")

      val node = new ConstellationNode(
        keyPair,
        seeds,
        config.getString("http.interface"),
        config.getInt("http.port"),
        config.getString("udp.interface"),
        config.getInt("udp.port"),
        timeoutSeconds = rpcTimeout,
        hostName = hostName,
        requestExternalAddressCheck = requestExternalAddressCheck
      )
    } match {
      case Failure(e) => e.printStackTrace()
      case Success(x) => logger.info("success")
    }

  }



}

class ConstellationNode(val configKeyPair: KeyPair,
                        val seedPeers: Seq[InetSocketAddress],
                        val httpInterface: String,
                        val httpPort: Int,
                        val udpInterface: String = "0.0.0.0",
                        val udpPort: Int = 16180,
                        val hostName: String = "127.0.0.1",
                        val timeoutSeconds: Int = 480,
                        val requestExternalAddressCheck : Boolean = false,
                        val autoSetExternalAddress: Boolean = false,
                        val peerHttpPort: Int = 9001,
                        val peerTCPPort: Int = 9002)(
                         implicit val system: ActorSystem,
                         implicit val materialize: ActorMaterializer,
                         implicit val executionContext: ExecutionContext
                       ){

  implicit val dao: DAO = new DAO()
  dao.updateKeyPair(configKeyPair)

  import dao._
  dao.actorMaterializer = materialize

  val logger = Logger(s"ConstellationNode_$publicKeyHash")

  logger.info("Node init")

  implicit val timeout: Timeout = Timeout(timeoutSeconds, TimeUnit.SECONDS)

  val udpAddressString: String = hostName + ":" + udpPort
  val udpAddress = new InetSocketAddress(hostName, udpPort)

  if (autoSetExternalAddress) {
    dao.externalAddress = Some(udpAddress)
    dao.apiAddress = Some(new InetSocketAddress(hostName, httpPort))
    dao.tcpAddress = Some(new InetSocketAddress(hostName, peerTCPPort))
  }

  val heartBeat: ActorRef = system.actorOf(
    Props(new Heartbeat(dao)), s"Heartbeat_$publicKeyHash"
  )
  dao.heartbeatActor = heartBeat


  val randomTX : ActorRef = system.actorOf(
    Props(new RandomTransactionManager(dao)), s"RandomTXManager_$publicKeyHash"
  )

  val cpUniqueSigner : ActorRef = system.actorOf(
    Props(new CheckpointUniqueSigner(dao)), s"CheckpointUniqueSigner_$publicKeyHash"
  )

  // Setup actors
  val metricsManager: ActorRef = system.actorOf(
    Props(new MetricsManager(dao)), s"MetricsManager_$publicKeyHash"
  )

  val memPoolManager: ActorRef = system.actorOf(
    Props(new MemPoolManager(metricsManager)), s"MemPoolManager_$publicKeyHash"
  )

  val peerManager: ActorRef = system.actorOf(
    Props(new PeerManager(dao)), s"PeerManager_$publicKeyHash"
  )

  val cellManager: ActorRef = system.actorOf(
    Props(new CellManager(memPoolManager, metricsManager, peerManager)), s"CellManager_$publicKeyHash"
  )

  val dbActor: ActorRef =  system.actorOf(
    Props(new LevelDBActor(dao)), s"ConstellationDBActor_$publicKeyHash"
  )

  val udpActor: ActorRef =
    system.actorOf(
      Props(new UDPActor(None, udpPort, udpInterface, dao)), s"ConstellationUDPActor_$publicKeyHash"
    )

  val consensusActor: ActorRef = system.actorOf(
    Props(new Consensus(dao)),
    s"ConstellationConsensusActor_$publicKeyHash")

  val edgeProcessorActor: ActorRef = system.actorOf(
    Props(new EdgeProcessor(dao)),
    s"ConstellationEdgeProcessorActor_$publicKeyHash")

  val cpMemPoolVerify: ActorRef = system.actorOf(
    Props(new CheckpointMemPoolVerifier(dao)),
    s"CheckpointMemPoolVerifier_$publicKeyHash")


  dao.dbActor = dbActor
  dao.consensus = consensusActor
  dao.peerManager = peerManager
  dao.metricsManager = metricsManager
  dao.edgeProcessor = edgeProcessorActor
  dao.cpSigner = cpUniqueSigner

  // If we are exposing rpc then create routes
  val routes: Route = new API(udpAddress, cellManager).authRoutes

  logger.info("API Binding")


  // Setup http server for internal API
  val bindingFuture: Future[Http.ServerBinding] = Http().bindAndHandle(routes, httpInterface, httpPort)

  val peerRoutes : Route = new PeerAPI(dao).routes

  // Setup http server for peer API
  Http().bindAndHandle(peerRoutes, httpInterface, peerHttpPort)

  def shutdown(): Unit = {
    udpActor ! Udp.Unbind

    bindingFuture
      .flatMap(_.unbind())
    // TODO: we should add this back but it currently causes issues in the integration test
    //.onComplete(_ => system.terminate())
  }

  //////////////

  // TODO : Move to separate test class - these are within jvm only but won't hurt anything
  // We could also consider creating a 'Remote Proxy class' that represents a foreign
  // ConstellationNode (i.e. the current Peer class) and have them under a common interface
  def getAPIClient(): APIClient = {
    val api = new APIClient().setConnection(host = hostName, port = httpPort)
    api.id = id
    api.udpPort = udpPort
    api
  }

  logger.info("Node started")

}
