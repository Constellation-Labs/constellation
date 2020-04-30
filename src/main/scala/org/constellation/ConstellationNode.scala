package org.constellation

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LoggingMagnet}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import better.files.File
import cats.Monad
import cats.effect.{Blocker, Clock, ConcurrentEffect, ContextShift, ExitCode, IO, IOApp, Resource, Sync, Timer}
import cats.implicits._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Decoder
import io.prometheus.client.CollectorRegistry
import org.constellation.CustomDirectives.printResponseTime
import org.constellation.datastore.SnapshotTrigger
import org.constellation.domain.configuration.{CliConfig, NodeConfig}
import org.constellation.infrastructure.configuration.CliConfigParser
import org.constellation.infrastructure.endpoints.middlewares.PeerAuthMiddleware
import org.constellation.infrastructure.endpoints.{
  BuildInfoEndpoints,
  CheckpointEndpoints,
  ClusterEndpoints,
  ConsensusEndpoints,
  MetricsEndpoints,
  NodeMetadataEndpoints,
  ObservationEndpoints,
  SignEndpoints,
  SnapshotEndpoints,
  SoeEndpoints,
  StatisticsEndpoints,
  TipsEndpoints,
  TransactionEndpoints,
  UIEndpoints
}
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import org.constellation.primitives.IPManager.IP
import org.constellation.primitives.Schema.{GenesisObservation, NodeState, ValidPeerIPData}
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.util.{AccountBalance, AccountBalanceCSVReader, Logging, Metrics}
import org.http4s.{HttpApp, HttpRoutes, Request}
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.okhttp.OkHttpBuilder
import org.slf4j.MDC
import org.http4s.implicits._
import org.http4s.metrics.prometheus.Prometheus
import org.http4s.syntax._
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.{Router, middleware, Server => H4Server}
import org.http4s.server.middleware.Logger
import pl.abankowski.httpsigner.http4s.{Http4sRequestSigner, Http4sResponseSigner}
import pl.abankowski.httpsigner.signature.generic.GenericGenerator

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.Try

/**
  * Main entry point for starting a node
  */
object ConstellationNode extends IOApp {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  final val LocalConfigFile = "local_config"
  final val preferencesPath = ".dag"

  import constellation._

  implicit val system: ActorSystem = ActorSystem("Constellation")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = ConstellationExecutionContext.bounded
  implicit val clock: Clock[IO] = Clock.create[IO]

  def run(args: List[String]): IO[ExitCode] = {
    val s = for {
      _ <- Resource.liftF(logger.info(s"Main init with args $args"))

      cliConfig <- Resource.liftF(CliConfigParser.parseCliConfig[IO](args))
      _ <- Resource.liftF(logger.info(s"CliConfig: $cliConfig"))

      config = ConfigFactory.load() // TODO

      _ <- Resource.liftF(createPreferencesPath[IO](preferencesPath))

      nodeConfig <- Resource.liftF(getNodeConfig[IO](cliConfig, config))

      requestMethodClassifier = (r: Request[IO]) => Some(r.method.toString.toLowerCase)

      registry <- Prometheus.collectorRegistry[IO]
      metrics <- Prometheus.metricsOps[IO](registry, "org_http4s_client")

      apiClient <- BlazeClientBuilder[IO](ConstellationExecutionContext.unbounded)
        .withConnectTimeout(30.seconds)
        .withMaxTotalConnections(1024)
        .withMaxWaitQueueLimit(512)
        .resource
        .map(org.http4s.client.middleware.Metrics[IO](metrics, requestMethodClassifier))

      logger = org.http4s.client.middleware.Logger[IO](logHeaders = true, logBody = false)(_)

      signedApiClient = PeerAuthMiddleware
        .requestSignerMiddleware(logger(apiClient), nodeConfig.primaryKeyPair.getPrivate, nodeConfig.hostName)

      node = new ConstellationNode(nodeConfig, signedApiClient, registry)

      server <- createServer(node.dao, registry)
    } yield server

    s.use(_ => IO.never).as(ExitCode.Success)
  }

  def getGenesisObservation(dao: DAO): Option[GenesisObservation] =
    dao.genesisObservation

  def createServer(dao: DAO, registry: CollectorRegistry): cats.effect.Resource[IO, Unit] =
    for {
      _ <- Resource.liftF(IO.unit)

      publicEndpoints = MetricsEndpoints.publicEndpoints[IO](dao.metrics) <+>
        NodeMetadataEndpoints.publicEndpoints[IO](dao.addressService) <+>
        TransactionEndpoints.publicEndpoints[IO](dao.transactionService, dao.checkpointBlockValidator) <+>
        ClusterEndpoints.publicEndpoints[IO](dao.cluster)

      peerPublicEndpoints = SignEndpoints.publicPeerEndpoints[IO](dao.keyPair, dao.cluster) <+>
        BuildInfoEndpoints.peerEndpoints[IO]() <+> MetricsEndpoints.peerEndpoints[IO](dao.metrics) <+>
        NodeMetadataEndpoints.peerEndpoints[IO](dao.cluster, dao.addressService, dao.addresses, dao.nodeType)

      peerWhitelistedEndpoints = CheckpointEndpoints.peerEndpoints[IO](
        getGenesisObservation(dao),
        dao.checkpointService,
        dao.checkpointAcceptanceService,
        dao.metrics,
        dao.snapshotService
      ) <+>
        ClusterEndpoints.peerEndpoints[IO](dao.cluster, dao.trustManager) <+>
        ConsensusEndpoints
          .peerEndpoints[IO](dao.consensusManager, dao.snapshotService, dao.transactionService) <+>
        ObservationEndpoints.peerEndpoints[IO](dao.observationService, dao.metrics) <+>
        SnapshotEndpoints.peerEndpoints[IO](
          dao.id,
          dao.snapshotStorage,
          dao.snapshotInfoStorage,
          dao.snapshotService,
          dao.cluster,
          dao.redownloadService
        ) <+>
        SoeEndpoints.peerEndpoints[IO](dao.soeService) <+>
        TipsEndpoints.peerEndpoints[IO](dao.id, dao.concurrentTipService, dao.checkpointService) <+>
        TransactionEndpoints.peerEndpoints[IO](dao.transactionService, dao.metrics)

      ownerEndpoints = BuildInfoEndpoints.ownerEndpoints[IO]() <+>
        MetricsEndpoints.ownerEndpoints[IO](dao.metrics) <+>
        UIEndpoints.ownerEndpoints[IO](dao.messageService) <+>
        StatisticsEndpoints.ownerEndpoints[IO](dao.recentBlockTracker, dao.transactionService, dao.cluster) <+>
        NodeMetadataEndpoints.ownerEndpoints[IO](dao.cluster, dao.addressService, dao.addresses, dao.nodeType) <+>
        SignEndpoints.ownerEndpoints[IO](dao.cluster) <+>
        SnapshotEndpoints.ownerEndpoints[IO](dao.snapshotStorage, dao.redownloadService)

      getKnownPeerId = { (ip: IP) =>
        dao.cluster.getPeerData(ip).map(_.map(_.peerMetadata.id))
      }
      getWhitelistedPeerId = { (ip: IP) =>
        dao.nodeConfig.whitelisting.get(ip).pure[IO]
      }

      peerEndpoints = PeerAuthMiddleware.requestVerifierMiddleware(getWhitelistedPeerId)(
        peerPublicEndpoints <+> PeerAuthMiddleware.enforceKnownPeersMiddleware(getWhitelistedPeerId, getKnownPeerId)(
          peerWhitelistedEndpoints
        )
      )

      signedPeerEndpoints = PeerAuthMiddleware.responseSignerMiddleware(dao.keyPair.getPrivate)(peerEndpoints)

      metrics <- Prometheus.metricsOps[IO](registry)
      metricsMiddleware = middleware.Metrics[IO](metrics)(_)

      publicRouter = Router("" -> metricsMiddleware(publicEndpoints)).orNotFound
      peerRouter = Router("" -> metricsMiddleware(signedPeerEndpoints)).orNotFound
      ownerRouter = Router("" -> metricsMiddleware(ownerEndpoints)).orNotFound

      responseLogger = middleware.Logger.httpApp[IO](logHeaders = true, logBody = false)(_)

      _ <- BlazeServerBuilder[IO]
        .bindHttp(9000, "0.0.0.0")
        .withHttpApp(responseLogger(publicRouter))
        .withoutBanner
        .resource

      _ <- BlazeServerBuilder[IO]
        .bindHttp(9001, "0.0.0.0")
        .withHttpApp(responseLogger(peerRouter))
        .withoutBanner
        .resource

      _ <- BlazeServerBuilder[IO]
        .bindHttp(9002, "0.0.0.0")
        .withHttpApp(responseLogger(ownerRouter))
        .withoutBanner
        .resource
    } yield ()

  private def getHostName[F[_]: Sync](cliConfig: CliConfig): F[String] = {
    import io.circe.generic.semiauto._
    import io.circe.parser.parse

    implicit val localNodeConfigDecoder: Decoder[LocalNodeConfig] = deriveDecoder[LocalNodeConfig]

    Sync[F].delay {
      Option(cliConfig.externalIp)
        .map(_.getHostAddress)
        .getOrElse(
          parse(File(LocalConfigFile).lines.mkString).toOption
            .map(_.as[LocalNodeConfig])
            .flatMap(_.toOption)
            .map(_.externalIP)
            .getOrElse("127.0.0.1")
        )
    }
  }

  private def getWhitelisting[F[_]: Sync](cliConfig: CliConfig): F[Map[IP, Id]] =
    for {
      source <- Sync[F].delay { Source.fromFile(cliConfig.whitelisting) }
      lines = source.getLines().filter(_.nonEmpty).toList
      values = lines.map(_.split(",").map(_.trim).toList)
      mappedValues = values.map {
        case ip :: id :: Nil => Map(ip -> Id(id))
        case _               => Map.empty[IP, Id]
      }.fold(Map.empty[IP, Id])(_ ++ _)
      _ <- Sync[F].delay { source.close() }
    } yield mappedValues

  private def getAllocAccountBalances[F[_]: Sync](cliConfig: CliConfig): F[Seq[AccountBalance]] = Sync[F].delay {
    Try(new AccountBalanceCSVReader(cliConfig.allocFilePath, cliConfig.allocFileNormalized).read()).getOrElse(Seq.empty)
  }

  private def createPreferencesPath[F[_]: Sync](path: String) = Sync[F].delay {
    File(path).createDirectoryIfNotExists()
  }

  private def getPort[F[_]: Sync](config: Config, fromArg: Option[Int], env: String, configPath: String): F[Int] =
    Sync[F].delay {
      fromArg.getOrElse(Option(System.getenv(env)).map(_.toInt).getOrElse(config.getInt(configPath)))
    }

  private def getKeyPair[F[_]: Sync](cliConfig: CliConfig): F[KeyPair] =
    KeyStoreUtils
      .keyPairFromStorePath(cliConfig.keyStorePath, cliConfig.alias)
      .value
      .flatMap({
        case Right(keyPair) => keyPair.pure[F]
        case Left(e)        => e.raiseError[F, KeyPair]
      })

  private def getNodeConfig[F[_]: Sync](cliConfig: CliConfig, config: Config): F[NodeConfig] =
    for {
      seeds <- CliConfigParser.loadSeedsFromConfig(config)
      logger <- Slf4jLogger.create[F]

      _ <- logger.debug(s"Seeds: $seeds")

      keyPair <- getKeyPair(cliConfig)

      hostName <- getHostName(cliConfig)

      portOffset = Option(cliConfig.externalPort).filter(_ != 0)
      httpPortFromArg = portOffset
      peerHttpPortFromArg = portOffset.map(_ + 1)

      httpPort <- getPort(config, httpPortFromArg, "DAG_HTTP_PORT", "http.port")
      peerHttpPort <- getPort(config, peerHttpPortFromArg, "DAG_PEER_HTTP_PORT", "http.peer-port")

      allocAccountBalances <- getAllocAccountBalances(cliConfig)
      _ <- logger.debug(s"Alloc: $allocAccountBalances")

      constellationConfig = config.getConfig("constellation")
      processingConfig = ProcessingConfig(maxWidth = constellationConfig.getInt("max-width"))

      whitelisting <- getWhitelisting(cliConfig)

      nodeConfig = NodeConfig(
        seeds = seeds,
        primaryKeyPair = keyPair,
        isGenesisNode = cliConfig.genesisNode,
        isLightNode = cliConfig.lightNode,
        isRollbackNode = cliConfig.rollbackNode,
        rollbackHeight = cliConfig.rollbackHeight,
        rollbackHash = cliConfig.rollbackHash,
        hostName = hostName,
        httpInterface = config.getString("http.interface"),
        httpPort = httpPort,
        peerHttpPort = peerHttpPort,
        defaultTimeoutSeconds = config.getInt("default-timeout-seconds"),
        attemptDownload = !cliConfig.genesisNode,
        cliConfig = cliConfig,
        processingConfig =
          if (cliConfig.testMode) ProcessingConfig.testProcessingConfig.copy(maxWidth = 10) else processingConfig,
        dataPollingManagerOn = config.getBoolean("constellation.dataPollingManagerOn"),
        allocAccountBalances = allocAccountBalances,
        whitelisting = whitelisting
      )
    } yield nodeConfig
}

class ConstellationNode(
  val nodeConfig: NodeConfig = NodeConfig(),
  client: Client[IO],
  registry: CollectorRegistry
)(
  implicit val system: ActorSystem,
  implicit val materialize: ActorMaterializer,
  implicit val C: ContextShift[IO]
//  implicit val executionContext: ExecutionContext
) extends StrictLogging {

  implicit val dao: DAO = new DAO()
  dao.apiClient = ClientInterpreter[IO](client)
  dao.nodeConfig = nodeConfig
  dao.metrics = new Metrics(registry, periodSeconds = nodeConfig.processingConfig.metricCheckInterval)
  dao.initialize(nodeConfig)

  dao.node = this

  MDC.put("node_id", dao.id.short)

  dao.eigenTrust.initializeModel().unsafeRunSync()
  dao.eigenTrust.registerSelf().unsafeRunSync()

  logger.info(
    s"Node init with API ${nodeConfig.httpInterface} ${nodeConfig.httpPort} peerPort: ${nodeConfig.peerHttpPort}"
  )

  val ipManager: IPManager[IO] =
    IPManager[IO]()(IO.ioConcurrentEffect(IO.contextShift(ConstellationExecutionContext.bounded)))

  // TODO: Unused, can be used for timing information but adds a lot to logs
  private val logReqResp: Directive0 = DebuggingDirectives.logRequestResult(
    LoggingMagnet(printResponseTime(logger))
  )

  def getIPData: ValidPeerIPData =
    ValidPeerIPData(nodeConfig.hostName, nodeConfig.peerHttpPort)

  def getInetSocketAddress: InetSocketAddress =
    new InetSocketAddress(nodeConfig.hostName, nodeConfig.peerHttpPort)

  def shutdown(): Unit = {

    val unbindTimeout = ConfigUtil.getDurationFromConfig("akka.http.unbind-api-timeout")

    val gracefulShutdown = IO(logger.info("Shutdown procedure starts")) >>
      IO.fromFuture(IO(system.terminate())) >>
      IO(logger.info("Shutdown completed"))

    dao.cluster
      .leave(gracefulShutdown.void)
      .unsafeRunSync()
  }

  //////////////

  logger.info("Node started")

  // TODO: Use this for full flow, right now this only works as a debugging measure, does not integrate properly
  // with other nodes joining
  if (nodeConfig.isGenesisNode) {
    logger.info("Creating genesis block")
    Genesis.start()
    logger.info(s"Genesis block hash ${dao.genesisBlock.map {
      _.soeHash
    }.getOrElse("")}")
    dao.cluster.compareAndSet(NodeState.initial, NodeState.Ready).unsafeRunAsync(_ => ())
  } else if (nodeConfig.isRollbackNode) {
    logger.info(s"Performing rollback for height: ${nodeConfig.rollbackHeight}, hash: ${nodeConfig.rollbackHash}")
    dao.rollbackService
      .restore(nodeConfig.rollbackHeight, nodeConfig.rollbackHash)
      .rethrowT
      .handleError(e => logger.error(s"Rollback error: ${Logging.stringifyStackTrace(e)}"))
      .unsafeRunSync()
    dao.cluster.compareAndSet(NodeState.initial, NodeState.Ready).unsafeRunAsync(_ => ())
  }

  // We're disabling auto rejoin after node start.
  // dao.cluster.initiateRejoin().unsafeRunSync

//  Keeping disabled for now -- going to only use midDb for the time being.
//  private val txMigrator = new TransactionPeriodicMigration
}
