package org.constellation

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.io.Udp
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.{CheckpointMemPoolVerifier, Consensus, EdgeProcessor}
import org.constellation.crypto.KeyUtils
import org.constellation.p2p.{PeerAPI, RegisterNextActor, UDPActor}
import org.constellation.primitives.Schema.{AddPeerFromLocal, ToggleHeartbeat}
import org.constellation.primitives._
import org.constellation.util.{APIClient, Heartbeat}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

object ConstellationNode extends App {

  implicit val system: ActorSystem = ActorSystem("Constellation")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("main-dispatcher")

  val config = ConfigFactory.load()

  val rpcTimeout = config.getInt("rpc.timeout")

  val seeds = args.headOption.map(_.split(",").map{constellation.addressToSocket}.toSeq).getOrElse(Seq())

  private val hostName = if (args.length > 1) {
    args(1)
  } else "127.0.0.1"

  private val requestExternalAddressCheck = if (args.length > 2) {
    args(2).toBoolean
  } else false

  // TODO: update to take from config
  val keyPair = KeyUtils.makeKeyPair()

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

  val data = new Data()
  data.updateKeyPair(configKeyPair)

  import data._
  data.actorMaterializer = materialize

  val logger = Logger(s"ConstellationNode_$publicKeyHash")

  implicit val timeout: Timeout = Timeout(timeoutSeconds, TimeUnit.SECONDS)

  val udpAddressString: String = hostName + ":" + udpPort
  val udpAddress = new InetSocketAddress(hostName, udpPort)

  if (autoSetExternalAddress) {
    data.externalAddress = Some(udpAddress)
    data.apiAddress = Some(new InetSocketAddress(hostName, httpPort))
    data.tcpAddress = Some(new InetSocketAddress(hostName, peerTCPPort))
  }

  val heartBeat: ActorRef = system.actorOf(
    Props(new Heartbeat(data)), s"Heartbeat_$publicKeyHash"
  )
  data.heartbeatActor = heartBeat


  val randomTX : ActorRef = system.actorOf(
    Props(new RandomTransactionManager(data)), s"RandomTXManager_$publicKeyHash"
  )

  // Setup actors
  val metricsManager: ActorRef = system.actorOf(
    Props(new MetricsManager(data)), s"MetricsManager_$publicKeyHash"
  )

  val memPoolManager: ActorRef = system.actorOf(
    Props(new MemPoolManager(metricsManager)), s"MemPoolManager_$publicKeyHash"
  )

  val peerManager: ActorRef = system.actorOf(
    Props(new PeerManager(data)), s"PeerManager_$publicKeyHash"
  )

  val cellManager: ActorRef = system.actorOf(
    Props(new CellManager(memPoolManager, metricsManager, peerManager)), s"CellManager_$publicKeyHash"
  )

  val dbActor: ActorRef =  system.actorOf(
    Props(new LevelDBActor(data)), s"ConstellationDBActor_$publicKeyHash"
  )

  val udpActor: ActorRef =
    system.actorOf(
      Props(new UDPActor(None, udpPort, udpInterface, data)), s"ConstellationUDPActor_$publicKeyHash"
    )

  val consensusActor: ActorRef = system.actorOf(
    Props(new Consensus(data)),
    s"ConstellationConsensusActor_$publicKeyHash")

  val edgeProcessorActor: ActorRef = system.actorOf(
    Props(new EdgeProcessor(data)),
    s"ConstellationEdgeProcessorActor_$publicKeyHash")

  val cpMemPoolVerify: ActorRef = system.actorOf(
    Props(new CheckpointMemPoolVerifier(data)),
    s"CheckpointMemPoolVerifier_$publicKeyHash")


  data.dbActor = dbActor
  data.consensus = consensusActor
  data.peerManager = peerManager
  data.metricsManager = metricsManager
  data.edgeProcessor = edgeProcessorActor

  // If we are exposing rpc then create routes
  val routes: Route = new API(udpAddress, data, cellManager).authRoutes

  // Setup http server for internal API
  val bindingFuture: Future[Http.ServerBinding] = Http().bindAndHandle(routes, httpInterface, httpPort)

  val peerRoutes : Route = new PeerAPI(data).routes

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
