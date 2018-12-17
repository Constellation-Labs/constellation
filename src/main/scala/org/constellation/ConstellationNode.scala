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
import org.constellation.consensus.Consensus
import org.constellation.crypto.KeyUtils
import org.constellation.p2p.{PeerAPI, PeerToPeer, RegisterNextActor, UDPActor}
import org.constellation.primitives.Schema.{AddPeerFromLocal, ToggleHeartbeat}
import org.constellation.primitives._
import org.constellation.util.Http4sClient

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

object ConstellationNode extends App {

  implicit val system: ActorSystem = ActorSystem("Constellation")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val config = ConfigFactory.load()

  val rpcTimeout = config.getInt("blockchain.defaultRPCTimeoutSeconds")

  val seeds = args.headOption.map(_.split(",").map{constellation.addressToSocket}.toSeq).getOrElse(Seq())

  private val hostName = if (args.length > 1) {
    args(1)
  } else "127.0.0.1"

  private val requestExternalAddressCheck = if (args.length > 2) {
    args(2).toBoolean
  } else false

  // TODO: update to take from config
  val keyPair = KeyUtils.makeKeyPair()

  // TODO: add seeds from config
  val node = new ConstellationNode(
    keyPair,
    seeds,
    config.getString("http.interface"),
    config.getInt("http.port"),
    config.getString("udp.interface"),
    config.getInt("udp.port"),
    timeoutSeconds = rpcTimeout,
    heartbeatEnabled = true,
    hostName = hostName,
    requestExternalAddressCheck = requestExternalAddressCheck
  )

  node.data.minGenesisDistrSize = 3

}

class ConstellationNode(
                         val configKeyPair: KeyPair,
                         val seedPeers: Seq[InetSocketAddress],
                         val httpInterface: String,
                         val httpPort: Int,
                         val udpInterface: String = "0.0.0.0",
                         val udpPort: Int = 16180,
                         val hostName: String = "127.0.0.1",
                         timeoutSeconds: Int = 30,
                         heartbeatEnabled: Boolean = false,
                         requestExternalAddressCheck : Boolean = false,
                         generateRandomTransactions: Boolean = true,
                         autoSetExternalAddress: Boolean = false,
                         val peerHttpPort: Int = 9001,
                         val peerTCPPort: Int = 9002
             )(
               implicit val system: ActorSystem,
               implicit val materialize: ActorMaterializer,
               implicit val executionContext: ExecutionContextExecutor
             ){

  val data = new Data()
  data.updateKeyPair(configKeyPair)
  import data._
  data.actorMaterializer = materialize

  generateRandomTX = generateRandomTransactions
  val logger = Logger(s"ConstellationNode_$publicKeyHash")

 // logger.info(s"UDP Info - hostname: $hostName interface: $udpInterface port: $udpPort")

  implicit val timeout: Timeout = Timeout(timeoutSeconds, TimeUnit.SECONDS)

  // TODO: add root actor for routing

  // TODO: in our p2p connection we need a blacklist of connections,
  // and need to make sure someone can't access internal actors

  // Setup actors

  val udpAddressString: String = hostName + ":" + udpPort
  val udpAddress = new InetSocketAddress(hostName, udpPort)

  if (autoSetExternalAddress) {
    data.externalAddress = Some(udpAddress)
    data.apiAddress = Some(new InetSocketAddress(hostName, httpPort))
    data.tcpAddress = Some(new InetSocketAddress(hostName, peerTCPPort))
  }

  val metricsManager: ActorRef = system.actorOf(
    Props(new MetricsManager()), s"MetricsManager_$publicKeyHash"
  )

  val memPoolManager: ActorRef = system.actorOf(
    Props(new MemPoolManager(metricsManager)), s"MemPoolManager_$publicKeyHash"
  )

  val keyManager: ActorRef = system.actorOf(
    Props(new KeyManager(data.keyPair, memPoolManager, metricsManager)), s"KeyManager_$publicKeyHash"
  )

  val peerManager: ActorRef = system.actorOf(
    Props(new PeerManager()), s"PeerManager_$publicKeyHash"
  )

  val cellManager: ActorRef = system.actorOf(
    Props(new CellManager(memPoolManager, metricsManager, peerManager)), s"CellManager_$publicKeyHash"
  )

  val nodeManager: ActorRef = system.actorOf(
    Props(new NodeManager(keyManager, metricsManager)), s"NodeManager_$publicKeyHash"
  )

  val randomTransactionManager: ActorRef = system.actorOf(
    Props(new RandomTransactionManager(nodeManager, peerManager, metricsManager)), s"RandomTransactionManager_$publicKeyHash"
  )

  val dbActor: ActorRef =  system.actorOf(
    Props(new LevelDBActor(data)), s"ConstellationDBActor_$publicKeyHash"
  )


  val udpActor: ActorRef =
    system.actorOf(
      Props(new UDPActor(None, udpPort, udpInterface, Some(data))), s"ConstellationUDPActor_$publicKeyHash"
    )

  val consensusActor: ActorRef = system.actorOf(
    Props(new Consensus(configKeyPair, udpActor)(timeout)),
    s"ConstellationConsensusActor_$publicKeyHash")

  val peerToPeerActor: ActorRef =
    system.actorOf(Props(new PeerToPeer(
      configKeyPair.getPublic, system,
      consensusActor, udpActor, data, requestExternalAddressCheck, heartbeatEnabled=heartbeatEnabled, randomTransactionManager, cellManager)
    (timeout)), s"ConstellationP2PActor_$publicKeyHash")

  data.p2pActor = Some(peerToPeerActor)
  data.dbActor = Some(dbActor)

  private val register = RegisterNextActor(peerToPeerActor)

  udpActor ! register

  // Seed peers
  if (seedPeers.nonEmpty) {
    seedPeers.foreach(peer => {
      logger.info(s"Attempting to connect to seed-host $peer")
      peerToPeerActor ! AddPeerFromLocal(peer)
    })
  }

  // If we are exposing rpc then create routes
  val routes: Route = new API(
    peerToPeerActor, consensusActor, udpAddress, data, peerManager,
    metricsManager, nodeManager, cellManager)(executionContext, timeout).authRoutes

  // Setup http server for internal API
  Http().bindAndHandle(routes, httpInterface, httpPort)

  val peerRoutes : Route = new PeerAPI(dbActor, nodeManager, keyManager).routes

  // Setup http server for peer API
  Http().bindAndHandle(peerRoutes, httpInterface, peerHttpPort)


  // TODO : Move to separate test class - these are within jvm only but won't hurt anything
  // We could also consider creating a 'Remote Proxy class' that represents a foreign
  // ConstellationNode (i.e. the current Peer class) and have them under a common interface


  def getAPIClient(): Http4sClient = {
    val api = new Http4sClient(port=Some(httpPort))
    api.id = id
    api.udpPort = udpPort
    api.peerHttpPort = peerHttpPort
    api
  }

  def healthy: Boolean = Try{getAPIClient().getBlocking[String]("health") == "OK"}.getOrElse(false)
  def add(other: ConstellationNode) = getAPIClient().postBlocking[String]("peer", body = other.udpAddressString)

  def shutdown(): Unit = {
    udpActor ! Udp.Unbind
    peerToPeerActor ! ToggleHeartbeat
  }

}
