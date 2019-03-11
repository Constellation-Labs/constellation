package org.constellation

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.RemoteAddress
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LoggingMagnet}
import akka.http.scaladsl.server.{Directive0, Route}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import better.files._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{Logger, StrictLogging}
import constellation._
import org.constellation.CustomDirectives.printResponseTime
import org.constellation.crypto.KeyUtils
import org.constellation.datastore.SnapshotTrigger
import org.constellation.p2p.PeerAPI
import org.constellation.primitives.Schema.{NodeState, ValidPeerIPData}
import org.constellation.primitives._
import org.constellation.util.{APIClient, Metrics}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// scopt requires default args for all properties.
// Make sure to check for null early -- don't propogate nulls anywhere else.
case class CliConfig(
                      externalIp: java.net.InetAddress = null,
                      externalPort: Int = 0,
                      debug: Boolean = false,
                      startOfflineMode: Boolean = false,
                      lightNode: Boolean = false,
                      genesisNode: Boolean = false
                    )

/**
  * Main entry point for starting a node
  */
object ConstellationNode extends StrictLogging {

  final val LocalConfigFile = "local_config"

  //noinspection ScalaStyle
  def main(args: Array[String]): Unit = {
    logger.info(s"Main init with args $args")

    import scopt.OParser
    val builder = OParser.builder[CliConfig]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("constellation"),
        head("constellation", BuildInfo.version),
        opt[java.net.InetAddress]("ip")
          .action((x, c) => c.copy(externalIp = x))
          .valueName("<ip address>")
          .text("the ip you can be reached from outside"),
        opt[Int]('p', "port")
          .action((x, c) => c.copy(externalPort = x))
          .text("the port you can be reached from outside"),
        opt[Boolean]('d', "debug")
          .action((x, c) => c.copy(debug = x))
          .text("run the node in debug mode"),
        opt[Boolean]('o', "offline")
          .action((x, c) => c.copy(startOfflineMode = x))
          .text("Start the node in offline mode. Won't connect automatically"),
        opt[Boolean]('l', "light")
          .action((x, c) => c.copy(startOfflineMode = x))
          .text("Start a light node, only validates & stores portions of the graph"),
        opt[Boolean]('g', "genesis")
          .action((x, c) => c.copy(genesisNode = x))
          .text("Start in single node genesis mode"),
        help("help").text("prints this usage text"),
        version("version").text(s"Constellation v${BuildInfo.version}"),
        checkConfig(
          c =>
            if (c.externalIp == null ^ c.externalPort == 0) {
              failure("ip and port must either both be set, or neither.")
            } else success
        )
      )
    }

    // OParser.parse returns Option[Config]
    val cliConfig: CliConfig = OParser.parse(parser1, args, CliConfig()) match {
      case Some(c) => c
      case _       =>
        // arguments are bad, error message will have been displayed
        throw new RuntimeException("Invalid set of cli options")
    }

    val config = ConfigFactory.load()
    logger.info("Config loaded")


    Try {

      // TODO: Move to scopt above.
      val seedsFromConfig: Seq[HostPort] = PeerManager.loadSeedsFromConfig(config)

      logger.info(s"Seeds: $seedsFromConfig")

      // TODO: This should be unified as a single conf file
      val hostName = Option(cliConfig.externalIp).map(_.toString).getOrElse {
        Try { File(LocalConfigFile).lines.mkString.x[LocalNodeConfig].externalIP }.getOrElse("127.0.0.1")
      }

      val preferencesPath = File(".dag")
      preferencesPath.createDirectoryIfNotExists()

      // TODO: update to take from config
      val keyPair = KeyUtils.loadDefaultKeyPair()

      val portOffset = Option(cliConfig.externalPort).filter(_ != 0)
      val httpPortFromArg = portOffset.map { _ + 1 }
      val peerHttpPortFromArg = portOffset.map { _ + 2 }

      val httpPort = httpPortFromArg.getOrElse(
        Option(System.getenv("DAG_HTTP_PORT"))
          .map {
            _.toInt
          }
          .getOrElse(config.getInt("http.port"))
      )

      val peerHttpPort = peerHttpPortFromArg.getOrElse(
        Option(System.getenv("DAG_PEER_HTTP_PORT"))
          .map {
            _.toInt
          }
          .getOrElse(config.getInt("http.peer-port"))
      )

      implicit val system: ActorSystem = ActorSystem("Constellation")
      implicit val materializer: ActorMaterializer = ActorMaterializer()
      implicit val executionContext: ExecutionContext = ExecutionContext.global

      new ConstellationNode(
        NodeInitializationConfig(
          seeds = seedsFromConfig,
          primaryKeyPair = keyPair,
          isGenesisNode = cliConfig.startOfflineMode,
          hostName = hostName,
          httpInterface = config.getString("http.interface"),
          httpPort = httpPort,
          peerHttpPort = peerHttpPort,
          defaultTimeoutSeconds = config.getInt("default-timeout-seconds"),
          attemptDownload = true,
          cliConfig = cliConfig
        )
      )
    } match {
      case Failure(e) => e.printStackTrace()
      case Success(_) =>
        logger.info("success")

        // To stop daemon threads
        while (true) {
          Thread.sleep(60 * 1000)
        }

    }

  }

}

case class NodeInitializationConfig(
                                     seeds: Seq[HostPort] = Seq(),
                                     primaryKeyPair: KeyPair = KeyUtils.makeKeyPair(),
                                     isGenesisNode: Boolean = false,
                                     metricIntervalSeconds: Int = 60,
                                     hostName: String = "127.0.0.1",
                                     httpInterface: String = "0.0.0.0",
                                     httpPort: Int = 9000,
                                     peerHttpPort: Int = 9001,
                                     defaultTimeoutSeconds: Int = 10,
                                     attemptDownload: Boolean = false,
                                     allowLocalhostPeers: Boolean = false,
                                     cliConfig: CliConfig = CliConfig()
                                   )

class ConstellationNode(
                        val nodeConfig: NodeInitializationConfig = NodeInitializationConfig()
                       )(
  implicit val system: ActorSystem,
  implicit val materialize: ActorMaterializer,
  implicit val executionContext: ExecutionContext
) {

  implicit val dao: DAO = new DAO(nodeConfig)
  import nodeConfig._
  import dao._

  val metrics_ = new Metrics(periodSeconds = dao.processingConfig.metricCheckInterval)
  dao.metrics = metrics_

  val randomTXManager = new RandomTransactionManager()

  val snapshotTrigger = new SnapshotTrigger()

  val ipManager = IPManager()

  dao.actorMaterializer = materialize

  val logger = Logger(s"ConstellationNode_$publicKeyHash")

  logger.info(s"Node init with API $httpInterface $httpPort peerPort: $peerHttpPort")

  constellation.standardTimeout = Timeout(defaultTimeoutSeconds, TimeUnit.SECONDS)

  lazy val peerHostPort = HostPort(hostName, peerHttpPort)

  val peerManager: ActorRef = system.actorOf(
    Props(new PeerManager(ipManager)),
    s"PeerManager_$publicKeyHash"
  )

  // val dbActor = SwayDBDatastore(dao)

//  dao.dbActor = dbActor
  dao.peerManager = peerManager

  private val logReqResp: Directive0 = DebuggingDirectives.logRequestResult(
    LoggingMagnet(printResponseTime(logger))
  )

  // If we are exposing rpc then create routes
  val routes: Route = new API().routes // logReqResp { }

  logger.info("API Binding")

  // Setup http server for internal API
  private val bindingFuture: Future[Http.ServerBinding] =
    Http().bindAndHandle(routes, httpInterface, httpPort)

  val peerAPI = new PeerAPI(ipManager)

  /*  seedPeers.foreach {
    peer => ipManager.addKnownIP(RemoteAddress(peer))
  }*/

  def addAddressToKnownIPs(addr: ValidPeerIPData): Unit = {
    val remoteAddr = RemoteAddress(new InetSocketAddress(addr.canonicalHostName, addr.port))
    ipManager.addKnownIP(remoteAddr)
  }

  def getIPData: ValidPeerIPData = {
    ValidPeerIPData(hostName, peerHttpPort)
  }

  def getInetSocketAddress: InetSocketAddress = {
    new InetSocketAddress(hostName, peerHttpPort)
  }

  // Setup http server for peer API
  private val peerBindingFuture = Http().bind(nodeConfig.httpInterface, nodeConfig.peerHttpPort)
    .runWith(Sink foreach { conn =>
    val address =  conn.remoteAddress
    conn.handleWith(peerAPI.routes(address))
  })

  def shutdown(): Unit = {

    bindingFuture
      .foreach(_.unbind())

    // TODO: we should add this back but it currently causes issues in the integration test
    //.onComplete(_ => system.terminate())

    //TypedActor(system).stop(dbActor)
  }

  //////////////

  // TODO : Move to separate test class - these are within jvm only but won't hurt anything
  // We could also consider creating a 'Remote Proxy class' that represents a foreign
  // ConstellationNode (i.e. the current Peer class) and have them under a common interface

  def getAPIClient(host: String = hostName,
                   port: Int = httpPort): APIClient = {
    val api = APIClient(host, port)
    api.id = id
    api
  }

  // TODO: Change E2E to not use this but instead rely on peer discovery, need to send addresses there too
  def getAddPeerRequest: PeerMetadata = {
    PeerMetadata(hostName, peerHttpPort, dao.id, auxAddresses = dao.addresses, nodeType = dao.nodeType)
  }

  def getAPIClientForNode(node: ConstellationNode): APIClient = {
    val ipData = node.getIPData
    val api = APIClient(host = ipData.canonicalHostName, port = ipData.port)
    api.id = id
    api
  }

  logger.info("Node started")

  if (attemptDownload) {
    nodeConfig.seeds.foreach {
      dao.peerManager ! _
    }
    PeerManager.initiatePeerReload()(dao, dao.edgeExecutionContext)
  }

  // TODO: Use this for full flow, right now this only works as a debugging measure, does not integrate properly
  // with other nodes joining
  if (nodeConfig.isGenesisNode) {
    logger.info("Creating genesis block")
    Genesis.start()
    logger.info(s"Genesis block hash ${dao.genesisBlock.map { _.soeHash }.getOrElse("")}")
    dao.setNodeState(NodeState.Ready)
    dao.generateRandomTX = true
  }

}
