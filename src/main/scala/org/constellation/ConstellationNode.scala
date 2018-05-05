package org.constellation

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.io.Udp
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.constellation.rpc.RPCInterface
import org.constellation.wallet.KeyUtils
import org.constellation.consensus.Consensus
import org.constellation.consensus.Consensus.{RegisterP2PActor, ToggleHeartbeat}
import org.constellation.p2p.{PeerToPeer, RegisterNextActor, UDPActor}
import org.constellation.p2p.PeerToPeer.{AddPeerFromLocal, Id}
import org.constellation.state.{ChainStateManager, MemPoolManager}
import org.constellation.util.RPCClient

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
  new ConstellationNode(
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

}

class ConstellationNode(
               val keyPair: KeyPair,
               val seedPeers: Seq[InetSocketAddress],
               val httpInterface: String,
               val httpPort: Int,
               val udpInterface: String = "0.0.0.0",
               val udpPort: Int = 16180,
               val hostName: String = "127.0.0.1",
               timeoutSeconds: Int = 30,
               heartbeatEnabled: Boolean = false,
               requestExternalAddressCheck : Boolean = false
             )(
               implicit val system: ActorSystem,
               implicit val materialize: ActorMaterializer,
               implicit val executionContext: ExecutionContextExecutor
             ){

  val publicKeyHash: Int = keyPair.getPublic.hashCode()

  val id : Id = Id(keyPair.getPublic)

  val logger = Logger(s"ConstellationNode_$publicKeyHash")

 // logger.info(s"UDP Info - hostname: $hostName interface: $udpInterface port: $udpPort")

  implicit val timeout: Timeout = Timeout(timeoutSeconds, TimeUnit.SECONDS)

  // TODO: add root actor for routing

  // TODO: in our p2p connection we need a blacklist of connections,
  // and need to make sure someone can't access internal actors

  // Setup actors

  val udpAddressString: String = hostName + ":" + udpPort
  val udpAddress = new InetSocketAddress(hostName, udpPort)

  {
    import constellation._
   // logger.info(s"UDP inet pretty: ${pprintInet(udpAddress)}")
  }

  val udpActor: ActorRef =
    system.actorOf(
      Props(new UDPActor(None, udpPort, udpInterface)), s"ConstellationUDPActor_$publicKeyHash"
    )

  val memPoolManagerActor: ActorRef =
    system.actorOf(Props(new MemPoolManager), s"ConstellationMemPoolManagerActor_$publicKeyHash")

  val chainStateActor: ActorRef =
    system.actorOf(Props(new ChainStateManager(memPoolManagerActor: ActorRef, id)), s"ConstellationChainStateActor_$publicKeyHash")

  val consensusActor: ActorRef = system.actorOf(
    Props(new Consensus(memPoolManagerActor, chainStateActor, keyPair, udpAddress, udpActor, heartbeatEnabled)(timeout)),
    s"ConstellationConsensusActor_$publicKeyHash")

  val peerToPeerActor: ActorRef =
    system.actorOf(Props(new PeerToPeer(keyPair.getPublic, system, consensusActor, udpActor, udpAddress, keyPair, chainStateActor, memPoolManagerActor, requestExternalAddressCheck)
    (timeout)), s"ConstellationP2PActor_$publicKeyHash")

  private val register = RegisterNextActor(peerToPeerActor)
  // logger.info(s"Sending UDP Actor registration: $register")
  udpActor ! register

  consensusActor ! RegisterP2PActor(peerToPeerActor)


  // Seed peers
  if (seedPeers.nonEmpty) {
    seedPeers.foreach(peer => {
      logger.info(s"Attempting to connect to seed-host $peer")
      peerToPeerActor ! AddPeerFromLocal(peer)
    })
  } else {
   // logger.info("No seed host configured, waiting for messages.")
  }

  // If we are exposing rpc then create routes
  val routes: Route = new RPCInterface(chainStateActor,
    peerToPeerActor, memPoolManagerActor, consensusActor, udpAddress)(executionContext, timeout).routes

  // Setup http server for rpc
  Http().bindAndHandle(routes, httpInterface, httpPort)

  // TODO : Move to separate test class - these are within jvm only but won't hurt anything
  // We could also consider creating a 'Remote Proxy class' that represents a foreign
  // ConstellationNode (i.e. the current Peer class) and have them under a common interface
  val rpc = new RPCClient(port=httpPort)
  def healthy: Boolean = Try{rpc.getSync("health").status == StatusCodes.OK}.getOrElse(false)
  def add(other: ConstellationNode): HttpResponse = rpc.postSync("peer", other.udpAddressString)

  def shutdown(): Unit = {
    udpActor ! Udp.Unbind
    consensusActor ! ToggleHeartbeat
  }

}
