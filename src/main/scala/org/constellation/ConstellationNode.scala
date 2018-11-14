package org.constellation

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props, TypedActor}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.RemoteAddress
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LoggingMagnet}
import akka.http.scaladsl.server.{Directive0, Route}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.CustomDirectives.printResponseTime
import org.constellation.crypto.KeyUtils
import org.constellation.datastore.swaydb.SwayDBDatastore
import org.constellation.p2p.PeerAPI
import org.constellation.primitives.Schema.ValidPeerIPData
import org.constellation.primitives._
import org.constellation.util.{APIClient, Heartbeat}
import org.joda.time.DateTime

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

      val preferencesPath = File(".dag")
      preferencesPath.createDirectoryIfNotExists()

      val _ = KeyUtils.provider // Ensure initialized

      val hostName = Try{File("external_host_ip").lines.mkString}.getOrElse("127.0.0.1")


      val keyPairPath = ".dag/key"
      val localKeyPair = Try{File(keyPairPath).lines.mkString.x[KeyPair]}

      localKeyPair match {
        case Failure(e) =>
          e.printStackTrace()
        case _ =>
      }


      // TODO: update to take from config
      logger.info("pre Key pair")
      val keyPair = {
        localKeyPair.getOrElse {
          logger.info(s"Key pair not found in $keyPairPath - Generating new key pair")
          val kp = KeyUtils.makeKeyPair()
          File(keyPairPath).write(kp.json)
          kp
        }
      }
      logger.info("post key pair")


      val portOffset = args.headOption.map{_.toInt}
      val httpPortFromArg = portOffset.map{_ + 1}
      val peerHttpPortFromArg = portOffset.map{_ + 2}

      val httpPort = httpPortFromArg.getOrElse(Option(System.getenv("DAG_HTTP_PORT")).map {
        _.toInt
      }.getOrElse(config.getInt("http.port")))

      val peerHttpPort = peerHttpPortFromArg.getOrElse(Option(System.getenv("DAG_PEER_HTTP_PORT")).map {
        _.toInt
      }.getOrElse(9001))

      val node = new ConstellationNode(
        keyPair,
        seeds,
        config.getString("http.interface"),
        httpPort,
        config.getString("udp.interface"),
        config.getInt("udp.port"),
        timeoutSeconds = rpcTimeout,
        hostName = hostName,
        requestExternalAddressCheck = requestExternalAddressCheck,
        peerHttpPort = peerHttpPort,
        attemptDownload = true,
        allowLocalhostPeers = false
      )
    } match {
      case Failure(e) => e.printStackTrace()
      case Success(_) => logger.info("success")

        while (true) {
          Thread.sleep(60*1000)
        }

    }

  }



}

class ConstellationNode(val configKeyPair: KeyPair,
                        val seedPeers: Seq[HostPort],
                        val httpInterface: String,
                        val httpPort: Int,
                        val udpInterface: String = "0.0.0.0",
                        val udpPort: Int = 16180,
                        val hostName: String = "127.0.0.1",
                        val timeoutSeconds: Int = 480,
                        val requestExternalAddressCheck : Boolean = false,
                        val autoSetExternalAddress: Boolean = false,
                        val peerHttpPort: Int = 9001,
                        val peerTCPPort: Int = 9002,
                        val attemptDownload: Boolean = false,
                        val allowLocalhostPeers: Boolean = false
                       )(
                         implicit val system: ActorSystem,
                         implicit val materialize: ActorMaterializer,
                         implicit val executionContext: ExecutionContext
                       ){

  implicit val dao: DAO = new DAO()
  dao.updateKeyPair(configKeyPair)
  dao.idDir.createDirectoryIfNotExists(createParents = true)

  dao.preventLocalhostAsPeer = !allowLocalhostPeers
  dao.externalHostString = hostName
  dao.externlPeerHTTPPort = peerHttpPort

  import dao._

  val heartBeat: ActorRef = system.actorOf(
    Props(new Heartbeat()), s"Heartbeat_$publicKeyHash"
  )

  dao.heartbeatActor = heartBeat
  // Setup actors
  val metricsManager: ActorRef = system.actorOf(
    Props(new MetricsManager()), s"MetricsManager_$publicKeyHash"
  )

  dao.metricsManager = metricsManager


  val ipManager = IPManager()

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

  val peerManager: ActorRef = system.actorOf(
    Props(new PeerManager(ipManager)), s"PeerManager_$publicKeyHash"
  )

 // val dbActor = SwayDBDatastore(dao)

//  dao.dbActor = dbActor
  dao.peerManager = peerManager

  private val logReqResp: Directive0 = DebuggingDirectives.logRequestResult(
    LoggingMagnet(printResponseTime(logger))
  )

  // If we are exposing rpc then create routes
  val routes: Route = new API(udpAddress).routes // logReqResp { }

  logger.info("API Binding")


  // Setup http server for internal API
  private val bindingFuture: Future[Http.ServerBinding] = Http().bindAndHandle(routes, httpInterface, httpPort)

  val peerAPI = new PeerAPI(ipManager)

  val peerRoutes : Route = peerAPI.routes  // logReqResp { }

/*  seedPeers.foreach {
    peer => ipManager.addKnownIP(RemoteAddress(peer))
  }*/

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

    bindingFuture
      .foreach(_.unbind())

    peerBindingFuture
      .foreach(_.unbind())
    // TODO: we should add this back but it currently causes issues in the integration test
    //.onComplete(_ => system.terminate())

    //TypedActor(system).stop(dbActor)
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

  def getAddPeerRequest: PeerMetadata = {
    PeerMetadata(hostName, udpPort, peerHttpPort, dao.id)
  }

  def getAPIClientForNode(node: ConstellationNode): APIClient = {
    val ipData = node.getIPData
    val api = APIClient(host = ipData.canonicalHostName, port = ipData.port)
    api.id = id
    api
  }

  logger.info("Node started")
  dao.metricsManager ! UpdateMetric("nodeState", dao.nodeState.toString)
  metricsManager ! UpdateMetric("address", dao.selfAddressStr)
  metricsManager ! UpdateMetric("nodeStartTimeMS", System.currentTimeMillis().toString)
  metricsManager ! UpdateMetric("nodeStartDate", new DateTime(System.currentTimeMillis()).toString)
  dao.metricsManager ! UpdateMetric("externalHost", dao.externalHostString)
  dao.metricsManager ! UpdateMetric("version", "1.0.9")

  if (attemptDownload) {
    seedPeers.foreach{
      dao.peerManager ! _
    }
    PeerManager.initiatePeerReload()(dao, dao.edgeExecutionContext)
  }

}
