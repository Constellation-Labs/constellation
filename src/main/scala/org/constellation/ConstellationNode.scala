package org.constellation

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LoggingMagnet}
import akka.http.scaladsl.server.{Directive0, Route}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import better.files._
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{Logger, StrictLogging}
import constellation._
import org.constellation.CustomDirectives.printResponseTime
import org.constellation.crypto.KeyUtils
import org.constellation.datastore.SnapshotTrigger
import org.constellation.p2p.{Cluster, PeerAPI}
import org.constellation.primitives.Schema.{NodeState, ValidPeerIPData}
import org.constellation.primitives._
import org.constellation.util.{APIClient, AccountBalance, AccountBalanceCSVReader, HostPort, Metrics}
import org.slf4j.MDC

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// scopt requires default args for all properties.
// Make sure to check for null early -- don't propogate nulls anywhere else.
case class CliConfig(
  externalIp: java.net.InetAddress = null,
  externalPort: Int = 0,
  allocFilePath: String = null,
  debug: Boolean = false,
  startOfflineMode: Boolean = false,
  lightNode: Boolean = false,
  genesisNode: Boolean = false,
  testMode: Boolean = false
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
        opt[String]('f', "path to file with allocation account balances")
          .action((x, c) => c.copy(allocFilePath = x))
          .text("path to file with allocation account balances"),
        opt[Unit]('d', "debug")
          .action((x, c) => c.copy(debug = true))
          .text("run the node in debug mode"),
        opt[Unit]('o', "offline")
          .action((x, c) => c.copy(startOfflineMode = true))
          .text("Start the node in offline mode. Won't connect automatically"),
        opt[Unit]('l', "light")
          .action((x, c) => c.copy(lightNode = true))
          .text("Start a light node, only validates & stores portions of the graph"),
        opt[Unit]('g', "genesis")
          .action((x, c) => c.copy(genesisNode = true))
          .text("Start in single node genesis mode"),
        opt[Unit]('t', "test-mode")
          .action((x, c) => c.copy(testMode = true))
          .text("Run with test settings"),
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
    logger.debug("Config loaded")

    Try {

      // TODO: Move to scopt above.
      val seedsFromConfig: Seq[HostPort] = Cluster.loadSeedsFromConfig(config)

      logger.debug(s"Seeds: $seedsFromConfig")

      // TODO: This should be unified as a single conf file
      val hostName = Option(cliConfig.externalIp).map(_.toString).getOrElse {
        Try { File(LocalConfigFile).lines.mkString.x[LocalNodeConfig].externalIP }
          .getOrElse("127.0.0.1")
      }

      val allocAccountBalances: Seq[AccountBalance] =
        Try(new AccountBalanceCSVReader(cliConfig.allocFilePath).read()).getOrElse(Seq.empty)
      logger.debug(s"Alloc: $allocAccountBalances")

      val preferencesPath = File(".dag")
      preferencesPath.createDirectoryIfNotExists()

      // TODO: update to take from config
      val keyPair = KeyUtils.loadDefaultKeyPair()

      val portOffset = Option(cliConfig.externalPort).filter(_ != 0)
      val httpPortFromArg = portOffset.map { _ + 1 }
      val peerHttpPortFromArg = portOffset.map { _ + 2 }

      val httpPort = httpPortFromArg.getOrElse(
        Option(System.getenv("DAG_HTTP_PORT")).map {
          _.toInt
        }.getOrElse(config.getInt("http.port"))
      )

      val peerHttpPort = peerHttpPortFromArg.getOrElse(
        Option(System.getenv("DAG_PEER_HTTP_PORT")).map {
          _.toInt
        }.getOrElse(config.getInt("http.peer-port"))
      )

      implicit val system: ActorSystem = ActorSystem("Constellation")
      implicit val materializer: ActorMaterializer = ActorMaterializer()
      implicit val executionContext: ExecutionContext = ConstellationExecutionContext.global

      val constellationConfig = config.getConfig("constellation")

      val processingConfig = ProcessingConfig(
        maxWidth = constellationConfig.getInt("max-width")
        // TODO: Finish porting configs from application conf
      )
      new ConstellationNode(
        NodeConfig(
          seeds = seedsFromConfig,
          primaryKeyPair = keyPair,
          isGenesisNode = cliConfig.genesisNode,
          isLightNode = cliConfig.lightNode,
          hostName = hostName,
          httpInterface = config.getString("http.interface"),
          httpPort = httpPort,
          peerHttpPort = peerHttpPort,
          defaultTimeoutSeconds = config.getInt("default-timeout-seconds"),
          attemptDownload = !cliConfig.genesisNode,
          cliConfig = cliConfig,
          processingConfig =
            if (cliConfig.testMode) ProcessingConfig.testProcessingConfig.copy(maxWidth = 10)
            else processingConfig,
          dataPollingManagerOn = config.getBoolean("constellation.dataPollingManagerOn"),
          allocAccountBalances = allocAccountBalances
        )
      )
    } match {
      case Failure(e) => e.printStackTrace()
      case Success(_) =>
        logger.debug("success")

        // To stop daemon threads
        while (true) {
          Thread.sleep(60 * 1000)
        }
    }
  }
}

case class NodeConfig(
  seeds: Seq[HostPort] = Seq(),
  primaryKeyPair: KeyPair = KeyUtils.makeKeyPair(),
  isGenesisNode: Boolean = false,
  isLightNode: Boolean = false,
  metricIntervalSeconds: Int = 60,
  hostName: String = "127.0.0.1",
  httpInterface: String = "0.0.0.0",
  httpPort: Int = 9000,
  peerHttpPort: Int = 9001,
  defaultTimeoutSeconds: Int = 90,
  attemptDownload: Boolean = false,
  allowLocalhostPeers: Boolean = false,
  cliConfig: CliConfig = CliConfig(),
  processingConfig: ProcessingConfig = ProcessingConfig(),
  dataPollingManagerOn: Boolean = false,
  allocAccountBalances: Seq[AccountBalance] = Seq.empty
)

class ConstellationNode(
  val nodeConfig: NodeConfig = NodeConfig()
)(
  implicit val system: ActorSystem,
  implicit val materialize: ActorMaterializer,
  implicit val executionContext: ExecutionContext
) {

  implicit val dao: DAO = new DAO()

  dao.initialize(nodeConfig)

  val logger = Logger(s"ConstellationNode_${dao.publicKeyHash}")
  MDC.put("node_id", dao.id.short)

  logger.info(
    s"Node init with API ${nodeConfig.httpInterface} ${nodeConfig.httpPort} peerPort: ${nodeConfig.peerHttpPort}"
  )

  dao.metrics = new Metrics(periodSeconds = dao.processingConfig.metricCheckInterval)

  val snapshotTrigger = new SnapshotTrigger(
    dao.processingConfig.snapshotTriggeringTimeSeconds
  )(dao, dao.cluster)

  val transactionGeneratorTrigger = new TransactionGeneratorTrigger(
    dao.processingConfig.randomTransactionLoopTimeSeconds
  )

  implicit val cs: ContextShift[IO] = ConstellationContextShift.global

  val ipManager = IPManager[IO]()

  nodeConfig.seeds.foreach { peer =>
    dao.ipManager.addKnownIP(peer.host)
  }

  // TODO: Unused, can be used for timing information but adds a lot to logs
  private val logReqResp: Directive0 = DebuggingDirectives.logRequestResult(
    LoggingMagnet(printResponseTime(logger))
  )

  // If we are exposing rpc then create routes
  val routes: Route = new API()(system, constellation.standardTimeout, dao).routes // logReqResp { }

  logger.info("Binding API")

  // Setup http server for internal API
  private val bindingFuture: Future[Http.ServerBinding] =
    Http().bindAndHandle(routes, nodeConfig.httpInterface, nodeConfig.httpPort)

  val peerAPI = new PeerAPI(dao.ipManager)

  def getIPData: ValidPeerIPData =
    ValidPeerIPData(nodeConfig.hostName, nodeConfig.peerHttpPort)

  def getInetSocketAddress: InetSocketAddress =
    new InetSocketAddress(nodeConfig.hostName, nodeConfig.peerHttpPort)

  // Setup http server for peer API
  // TODO: Add shutdown mechanism
  Http()
    .bind(nodeConfig.httpInterface, nodeConfig.peerHttpPort)
    .runWith(Sink.foreach { conn =>
      val address = conn.remoteAddress
      conn.handleWith(peerAPI.routes(address))
    })

  def shutdown(): Unit = {
    val gracefulShutdown = IO.delay(bindingFuture.foreach(_.unbind())) *>
      IO.delay(logger.info("Node shutdown completed"))

    dao.cluster
      .leave(gracefulShutdown)
      .unsafeRunSync()

    // TODO: we should add this back but it currently causes issues in the integration test
    //.onComplete(_ => system.terminate())

  }

  //////////////

  // TODO : Move to separate test class - these are within jvm only but won't hurt anything
  // We could also consider creating a 'Remote Proxy class' that represents a foreign
  // ConstellationNode (i.e. the current Peer class) and have them under a common interface

  def getAPIClient(host: String = nodeConfig.hostName, port: Int = nodeConfig.httpPort): APIClient = {
    val api = APIClient(host, port)
    api.id = dao.id
    api
  }

  def getPeerAPIClient: APIClient = {
    val api = APIClient(dao.nodeConfig.hostName, dao.nodeConfig.peerHttpPort)
    api.id = dao.id
    api
  }

  // TODO: Change E2E to not use this but instead rely on peer discovery, need to send addresses there too
  def getAddPeerRequest: PeerMetadata =
    PeerMetadata(
      nodeConfig.hostName,
      nodeConfig.peerHttpPort,
      dao.id,
      auxAddresses = dao.addresses,
      nodeType = dao.nodeType,
      resourceInfo = ResourceInfo(
        diskUsableBytes = new java.io.File(dao.snapshotPath.pathAsString).getUsableSpace
      )
    )

  def getAPIClientForNode(node: ConstellationNode): APIClient = {
    val ipData = node.getIPData
    val api = APIClient(host = ipData.canonicalHostName, port = ipData.port)
    api.id = dao.id
    api
  }

  logger.info("Node started")

  if (nodeConfig.attemptDownload) {
    (nodeConfig.seeds.toList.traverse(dao.cluster.hostPort) *> dao.cluster.initiatePeerReload()).unsafeRunSync
  }

  // TODO: Use this for full flow, right now this only works as a debugging measure, does not integrate properly
  // with other nodes joining
  if (nodeConfig.isGenesisNode) {
    logger.info("Creating genesis block")
    Genesis.start()
    logger.info(s"Genesis block hash ${dao.genesisBlock.map { _.soeHash }.getOrElse("")}")
    dao.cluster.setNodeState(NodeState.Ready).unsafeRunSync
    enableTransactionGenerator()
  }

//  Keeping disabled for now -- going to only use midDb for the time being.
//  private val txMigrator = new TransactionPeriodicMigration

  var dataPollingManager: DataPollingManager = _
  if (nodeConfig.dataPollingManagerOn) {
    dataPollingManager = new DataPollingManager(60)
  }

  private def enableTransactionGenerator(): Unit = {
    dao.generateRandomTX = true
    dao.metrics.updateMetric("generateRandomTX", dao.generateRandomTX.toString)
    logger.info("TransactionGenerator is enabled")
  }
}
