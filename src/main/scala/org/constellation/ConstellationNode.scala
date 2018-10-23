package org.constellation

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props, TypedActor, TypedProps}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.RemoteAddress
import akka.http.scaladsl.server.Route
import akka.io.Udp
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.{CheckpointMemPoolVerifier, CheckpointUniqueSigner, Consensus, EdgeProcessor}
import org.constellation.crypto.KeyUtils
import org.constellation.p2p.{PeerAPI, UDPActor}
import org.constellation.primitives.Schema.ValidPeerIPData
import org.constellation.primitives._
import org.constellation.util.{APIClient, Heartbeat}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object ConstellationNode {
import scala.concurrent.ExecutionContext

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
        Option(System.getenv("DAG_HTTP_PORT")).map{_.toInt}.getOrElse(config.getInt("http.port")),
        config.getString("udp.interface"),
        config.getInt("udp.port"),
        timeoutSeconds = rpcTimeout,
        hostName = hostName,
        requestExternalAddressCheck = requestExternalAddressCheck,
        peerHttpPort = Option(System.getenv("DAG_PEER_HTTP_PORT")).map{_.toInt}.getOrElse(9001)
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

  val ipManager = IPManager()

  import dao._
  dao.actorMaterializer = materialize

  val logger = Logger(s"ConstellationNode_$publicKeyHash")

  logger.info(s"Node init with API $httpInterface $httpPort peerPort: $peerHttpPort")

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
    Props(new MetricsManager()), s"MetricsManager_$publicKeyHash"
  )

  val memPoolManager: ActorRef = system.actorOf(
    Props(new MemPoolManager(metricsManager)), s"MemPoolManager_$publicKeyHash"
  )

  val peerManager: ActorRef = system.actorOf(
    Props(new PeerManager(ipManager, dao)), s"PeerManager_$publicKeyHash"
  )

  val dbActor: KVDB = TypedActor(system).typedActorOf(TypedProps(
    classOf[KVDB],
    new KVDBImpl(dao)), s"KVDB_$publicKeyHash")

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
  val routes: Route = new API(udpAddress).routes

  logger.info("API Binding")


  // Setup http server for internal API
  private val bindingFuture: Future[Http.ServerBinding] = Http().bindAndHandle(routes, httpInterface, httpPort)

  val peerAPI = new PeerAPI(ipManager)

  val peerRoutes : Route = peerAPI.routes


  seedPeers.foreach {
    peer => ipManager.addKnownIP(RemoteAddress(peer))
  }

  def addAddressToKnownIPs(addr: ValidPeerIPData): Unit = {
    val remoteAddr = RemoteAddress(new InetSocketAddress(addr.canonicalHostName, addr.port))
    ipManager.addKnownIP(remoteAddr)
  }

  def getIPData: ValidPeerIPData = {
    ValidPeerIPData(this.hostName, this.peerHttpPort)
  }

  def getInetSocketAddress: InetSocketAddress = {
    new InetSocketAddress(this.hostName, this.peerHttpPort)
  }


  // Setup http server for peer API
  private val peerBindingFuture = Http().bindAndHandle(peerRoutes, httpInterface, peerHttpPort)

  def shutdown(): Unit = {
    udpActor ! Udp.Unbind

    bindingFuture
      .foreach(_.unbind())

    peerBindingFuture
      .foreach(_.unbind())
    // TODO: we should add this back but it currently causes issues in the integration test
    //.onComplete(_ => system.terminate())

    TypedActor(system).stop(dbActor)
  }

  //////////////

  // TODO : Move to separate test class - these are within jvm only but won't hurt anything
  // We could also consider creating a 'Remote Proxy class' that represents a foreign
  // ConstellationNode (i.e. the current Peer class) and have them under a common interface
  def getAPIClient(host: String = hostName, port: Int = httpPort, udpPort: Int = udpPort): APIClient = {
    val api = APIClient(host, port, udpPort)
    api.id = id
    api
  }

  def getAddPeerRequest: AddPeerRequest = {
    AddPeerRequest(hostName, udpPort, peerHttpPort, dao.id)
  }

  def getAPIClientForNode(node: ConstellationNode): APIClient = {
    val ipData = node.getIPData
    val api = APIClient(host = ipData.canonicalHostName, port = ipData.port)
    api.id = id
    api
  }

  logger.info("Node started")

  metricsManager ! UpdateMetric("address", dao.selfAddressStr)

}
