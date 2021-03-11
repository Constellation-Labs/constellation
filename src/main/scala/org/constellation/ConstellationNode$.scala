package org.constellation

import java.security.KeyPair

import better.files.File
import cats.effect.{ExitCode, IO, IOApp, Resource, Sync, SyncIO}
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import fs2.Stream
import fs2.concurrent.Queue
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Decoder
import io.prometheus.client.CollectorRegistry
import org.constellation.ConstellationExecutionContext._
import org.constellation.domain.cloud.CloudService
import org.constellation.domain.cloud.CloudService.{CloudServiceEnqueue, DataToSend}
import org.constellation.domain.cloud.config.CloudConfig
import org.constellation.domain.configuration.{CliConfig, NodeConfig}
import org.constellation.genesis.Genesis
import org.constellation.infrastructure.configuration.CliConfigParser
import org.constellation.infrastructure.endpoints._
import org.constellation.infrastructure.endpoints.middlewares.PeerAuthMiddleware
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.keytool.KeyStoreUtils
import org.constellation.schema.checkpoint.CheckpointBlock
import org.constellation.schema.{GenesisObservation, Id, NodeState}
import org.constellation.session.SessionTokenService
import org.constellation.util.{AccountBalance, AccountBalanceCSVReader, HostPort, Metrics}
import org.http4s.Request
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.implicits._
import org.http4s.metrics.prometheus.Prometheus
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.{Router, middleware}
import org.slf4j.MDC
import pureconfig._
import pureconfig.generic.auto._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.io.Source
import scala.util.Try

object ConstellationNode$ extends IOApp with IOApp.WithContext {

  final val LocalConfigFile = "local_config"

  val logger = Slf4jLogger.getLogger[IO]

  override protected def executionContextResource: Resource[SyncIO, ExecutionContext] =
    unboundedResource

  override def run(args: List[String]): IO[ExitCode] =
    run$(args).compile.drain
      .as(ExitCode.Success)
      .handleErrorWith { err =>
        logger.error(err)(s"An error occurred during startup process.") >> ExitCode.Error.pure[IO]
      }

  def run$(args: List[String]) = {
    import constellation.PublicKeyExt
    for {
      _ <- Stream.eval(logger.info(s"Main init with args: $args"))

      bounded <- Stream.resource(boundedResource)
      unbounded = executionContext
      callbacks <- Stream.resource(callbacksResource)
      callbacksHealth <- Stream.resource(callbacksHealthResource)
      unboundedHealth <- Stream.resource(unboundedHealthResource)

      cliConfig <- getCLIConfig(args)
      config <- getAppConfig()
      cloudConfig <- getCloudConfig(cliConfig)
      nodeConfig <- getNodeConfig[IO](cliConfig, config)

      _ <- validateInitConditions(nodeConfig)

      registry <- Stream.resource(Prometheus.collectorRegistry[IO])
      metrics <- Stream.resource(Prometheus.metricsOps[IO](registry, "org_http4s_client"))
      sessionTokenService = new SessionTokenService[IO]()

      requestMethodClassifier = (r: Request[IO]) => Some(r.method.toString.toLowerCase)

      apiClient <- Stream.resource {
        BlazeClientBuilder[IO](unbounded)
          .withConnectTimeout(30.seconds)
          .withMaxTotalConnections(1024)
          .withMaxWaitQueueLimit(512)
          .resource
          .map(org.http4s.client.middleware.Metrics[IO](metrics, requestMethodClassifier))
      }.map { apiClient =>
        PeerAuthMiddleware
          .requestSignerMiddleware(
            org.http4s.client.middleware.Logger[IO](logHeaders = true, logBody = false)(apiClient),
            nodeConfig.primaryKeyPair.getPrivate,
            nodeConfig.hostName,
            sessionTokenService,
            nodeConfig.primaryKeyPair.getPublic.toId
          )
      }

      cloudService = CloudService[IO](cloudConfig)
      cloudQueue <- Stream.eval(Queue.unbounded[IO, DataToSend])
      cloudQueueInstance <- Stream.eval(cloudService.cloudSendingQueue(cloudQueue))

      dao <- runNode$(
        nodeConfig,
        cloudQueueInstance,
        apiClient,
        registry,
        sessionTokenService,
        bounded,
        unbounded,
        unboundedHealth
      )

      publicEndpoints = MetricsEndpoints.publicEndpoints[IO](dao.metrics) <+>
        NodeMetadataEndpoints.publicEndpoints[IO](dao.addressService, dao.cluster, dao.nodeType) <+>
        TransactionEndpoints.publicEndpoints[IO](dao.transactionService, dao.checkpointBlockValidator) <+>
        ClusterEndpoints.publicEndpoints[IO](dao.cluster, dao.trustManager) <+>
        CheckpointEndpoints.publicEndpoints[IO](dao.checkpointService) <+>
        ObservationEndpoints.publicEndpoints[IO](dao.observationService) <+>
        SnapshotEndpoints.publicEndpoints[IO](
          dao.id,
          dao.snapshotStorage,
          dao.snapshotService,
          dao.redownloadService
        ) <+>
        TipsEndpoints.publicEndpoints[IO](dao.id, dao.concurrentTipService, dao.checkpointService) <+>
        SoeEndpoints.publicEndpoints[IO](dao.soeService)

      peerPublicEndpoints = SignEndpoints.publicPeerEndpoints[IO](dao.keyPair, dao.cluster) <+>
        BuildInfoEndpoints.peerEndpoints[IO]() <+> MetricsEndpoints.peerEndpoints[IO](dao.metrics) <+>
        NodeMetadataEndpoints.peerEndpoints[IO](dao.cluster, dao.addressService, dao.nodeType)

      peerWhitelistedEndpoints = CheckpointEndpoints.peerEndpoints[IO](
        getGenesisObservation(dao),
        dao.checkpointService,
        dao.checkpointAcceptanceService,
        dao.metrics,
        dao.snapshotService
      ) <+>
        ClusterEndpoints.peerEndpoints[IO](dao.cluster, dao.trustManager) <+>
        ConsensusEndpoints
          .peerEndpoints[IO](dao.consensusManager, dao.snapshotService, dao.checkpointBlockValidator) <+>
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
        UIEndpoints.ownerEndpoints[IO](dao.messageService, unbounded) <+>
        StatisticsEndpoints.ownerEndpoints[IO](dao.recentBlockTracker, dao.transactionService, dao.cluster) <+>
        NodeMetadataEndpoints.ownerEndpoints[IO](dao.cluster, dao.addressService, dao.nodeType) <+>
        SignEndpoints.ownerEndpoints[IO](dao.cluster) <+>
        SnapshotEndpoints.ownerEndpoints[IO](dao.snapshotStorage, dao.redownloadService)

      getKnownPeerId = { (ip: String) =>
        dao.cluster
          .getPeerData(ip)
          .map(_.filter(pd => NodeState.canUseAPI(pd.peerMetadata.nodeState)).map(_.peerMetadata.id))
      }

      isIdWhitelisted = { (id: Id) =>
        dao.nodeConfig.whitelisting.contains(id)
      }

      peerEndpoints = PeerAuthMiddleware.requestVerifierMiddleware(getKnownPeerId, isIdWhitelisted)(
        peerPublicEndpoints <+> PeerAuthMiddleware.requestTokenVerifierMiddleware(dao.sessionTokenService)(
          PeerAuthMiddleware.enforceKnownPeersMiddleware(getKnownPeerId, isIdWhitelisted)(
            peerWhitelistedEndpoints
          )
        )
      )

      signedPeerEndpoints = PeerAuthMiddleware
        .responseSignerMiddleware(dao.keyPair.getPrivate, dao.sessionTokenService)(
          peerEndpoints
        )

      metrics <- Stream.resource(Prometheus.metricsOps[IO](registry))
      metricsMiddleware = middleware.Metrics[IO](metrics)(_)

      publicRouter = Router("" -> metricsMiddleware(publicEndpoints)).orNotFound
      peerRouter = Router("" -> metricsMiddleware(signedPeerEndpoints)).orNotFound
      ownerRouter = Router("" -> metricsMiddleware(ownerEndpoints)).orNotFound

      responseLogger = middleware.Logger.httpApp[IO](logHeaders = true, logBody = false)(_)

      signedPeerHealthCheckEndpoints = PeerAuthMiddleware
        .responseSignerMiddleware(dao.keyPair.getPrivate, dao.sessionTokenService)(
          PeerAuthMiddleware.requestVerifierMiddleware(getKnownPeerId, isIdWhitelisted)(
            // TODO: shouldn't THIS endpoint also be verified by verifiers below? if one node removes another by mistake the node it removed will not figure that out during healthcheck
            MetricsEndpoints.peerHealthCheckEndpoints[IO]() <+>
              PeerAuthMiddleware.requestTokenVerifierMiddleware(dao.sessionTokenService)(
                PeerAuthMiddleware.enforceKnownPeersMiddleware(getKnownPeerId, isIdWhitelisted)(
                  HealthCheckEndpoints.peerHealthCheckEndpoints[IO](dao.healthCheckConsensusManager)
                )
              )
          )
        )

      peerHealthCheckRouter = Router("" -> metricsMiddleware(signedPeerHealthCheckEndpoints)).orNotFound

      publicAPI = {
        implicit val cs = IO.contextShift(callbacks)
        implicit val ce = IO.ioConcurrentEffect(cs)

        BlazeServerBuilder[IO](callbacks)
          .bindHttp(9000, "0.0.0.0")
          .withHttpApp(responseLogger(publicRouter))
          .withoutBanner
          .serve
      }

      peerAPI = {
        implicit val cs = IO.contextShift(callbacks)
        implicit val ce = IO.ioConcurrentEffect(cs)

        BlazeServerBuilder[IO](callbacks)
          .bindHttp(9001, "0.0.0.0")
          .withHttpApp(responseLogger(peerRouter))
          .withoutBanner
          .serve
      }

      ownerAPI = {
        implicit val cs = IO.contextShift(callbacks)
        implicit val ce = IO.ioConcurrentEffect(cs)

        BlazeServerBuilder[IO](callbacks)
          .bindHttp(9002, "0.0.0.0")
          .withHttpApp(responseLogger(ownerRouter))
          .withoutBanner
          .serve
      }

      healthAPI = {
        implicit val cs = IO.contextShift(callbacksHealth)
        implicit val ce = IO.ioConcurrentEffect(cs)

        BlazeServerBuilder[IO](callbacksHealth)
          .bindHttp(nodeConfig.healthHttpPort, "0.0.0.0")
          .withHttpApp(responseLogger(peerHealthCheckRouter))
          .withoutBanner
          .serve
      }

      _ <- publicAPI.merge(peerAPI).merge(ownerAPI).merge(healthAPI)
    } yield ()
  }

  def runNode$(
    nodeConfig: NodeConfig,
    cloudService: CloudServiceEnqueue[IO],
    client: Client[IO],
    registry: CollectorRegistry,
    sessionTokenService: SessionTokenService[IO],
    boundedExecutionContext: ExecutionContext,
    unboundedExecutionContext: ExecutionContext,
    unboundedHealthExecutionContext: ExecutionContext
  ): Stream[IO, DAO] =
    for {
      _ <- Stream.eval(logger.info(s"Run node flow"))
      dao <- Stream.eval(IO {
        new DAO(boundedExecutionContext, unboundedExecutionContext, unboundedHealthExecutionContext)
      })
      _ <- Stream.eval(IO { dao.sessionTokenService = sessionTokenService })
      _ <- Stream.eval(IO {
        dao.apiClient = {
          val cs = IO.contextShift(unboundedExecutionContext)
          val ce = IO.ioConcurrentEffect(cs)

          ClientInterpreter[IO](client, dao.sessionTokenService)(ce, cs)
        }
      })
      _ <- Stream.eval(IO {
        dao.nodeConfig = nodeConfig
      })
      _ <- Stream.eval(IO {
        dao.metrics = new Metrics(
          registry,
          periodSeconds = nodeConfig.processingConfig.metricCheckInterval,
          unboundedExecutionContext
        )(dao)
      })
      _ <- Stream.eval(IO {
        dao.initialize(nodeConfig, cloudService)
      })
      _ <- Stream.eval(IO {
        MDC.put("node_id", dao.id.short)
      })
      _ <- Stream.eval(dao.eigenTrust.initializeModel())
      _ <- Stream.eval(dao.eigenTrust.registerSelf())

      _ <- Stream.eval(
        logger.info(
          s"Node init with API ${nodeConfig.httpInterface} ${nodeConfig.httpPort} peerPort: ${nodeConfig.peerHttpPort}"
        )
      )

      _ <- Stream.eval {
        if (nodeConfig.isGenesisNode) {
          logger.info("Creating genesis block") >>
            Genesis.start[IO](dao) >>
            logger.info(s"Genesis block ${dao.genesisBlock.map(CheckpointBlock.checkpointToJsonString).getOrElse("")}") >>
            dao.sessionTokenService.createAndSetNewOwnToken() >>
            dao.cluster.compareAndSet(NodeState.initial, NodeState.Ready)
        } else if (nodeConfig.isRollbackNode) {
          logger.info(s"Performing rollback for height: ${nodeConfig.rollbackHeight}, hash: ${nodeConfig.rollbackHash}") >>
            dao.rollbackService
              .restore(nodeConfig.rollbackHeight, nodeConfig.rollbackHash)
              .rethrowT
              .handleErrorWith(e => logger.error(e)(s"Rollback error")) >>
            dao.sessionTokenService.createAndSetNewOwnToken() >>
            dao.cluster.compareAndSet(NodeState.initial, NodeState.Ready).start
        } else IO.unit
      }
    } yield dao

  def getGenesisObservation(dao: DAO): Option[GenesisObservation] =
    dao.genesisObservation

  private def validateInitConditions(nodeConfig: NodeConfig): Stream[IO, Unit] = {
    import constellation.PublicKeyExt

    val minCpuCount = sys.env.get("CL_MIN_CPU_COUNT").flatMap(x => Try(x.toInt).toOption).getOrElse(4)

    for {
      _ <- Stream.eval {
        IO { File(s"tmp/${nodeConfig.primaryKeyPair.getPublic.toId.medium}/peers").nonEmpty }
          .ifM(IO.raiseError(new Throwable("Cannot start, tmp not cleared")), IO.unit)
      }

      _ <- Stream.eval {
        IO { new java.io.File(s"./").getUsableSpace }
          .map(_ / 1024d / 1024 / 1024) // to gigabytes
          .map(_ < nodeConfig.minRequiredSpace)
          .ifM(
            IO.raiseError(
              new Throwable(s"Cannot start, not enough space. Minimum required is ${nodeConfig.minRequiredSpace}Gb")
            ),
            IO.unit
          )
      }

      _ <- Stream.eval {
        IO { Runtime.getRuntime.availableProcessors }
          .map(_ >= minCpuCount)
          .ifM(
            IO.unit,
            IO.raiseError(
              new Throwable(s"Cannot start, at least 4 available CPU cores are required")
            )
          )
      }
    } yield ()
  }

  private def getCLIConfig(args: List[String]): Stream[IO, CliConfig] =
    Stream.eval(CliConfigParser.parseCliConfig[IO](args).flatTap { config =>
      logger.info(s"CliConfig: $config")
    })

  private def getAppConfig(): Stream[IO, Config] = Stream.eval(IO(ConfigFactory.load()))

  private def getCloudConfig(cliConfig: CliConfig): Stream[IO, CloudConfig] =
    Stream.eval {
      IO.pure(Option(cliConfig.cloud)).flatMap { c =>
        c.fold(CloudConfig.empty.pure[IO]) { c =>
          ConfigSource.file(c).load[CloudConfig] match {
            case Left(failures) =>
              IO.raiseError(
                new Throwable(
                  s"Cloud config file specified but loading failed, reason: ${failures.toList.map(_.description)}"
                )
              )
            case Right(config) =>
              config.pure[IO]
          }
        }
      }
    }

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

  private def getWhitelisting[F[_]: Sync](cliConfig: CliConfig): F[Map[Id, Option[String]]] =
    for {
      source <- Sync[F].delay {
        Source.fromFile(cliConfig.whitelisting)
      }
      lines = source.getLines().filter(_.nonEmpty).toList
      values = lines.map(_.split(",").map(_.trim).toList)
      mappedValues = values.map {
        case id :: alias :: Nil => Map(Id(id) -> Some(alias))
        case id :: Nil          => Map(Id(id) -> None)
        case _                  => Map.empty[Id, Option[String]]
      }.fold(Map.empty[Id, Option[String]])(_ ++ _)
      _ <- Sync[F].delay {
        source.close()
      }
    } yield mappedValues

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

  private def getAllocAccountBalances[F[_]: Sync](cliConfig: CliConfig): F[Seq[AccountBalance]] = Sync[F].delay {
    Try(new AccountBalanceCSVReader(cliConfig.allocFilePath, cliConfig.allocFileNormalized).read()).getOrElse(Seq.empty)
  }

  private def getNodeConfig[F[_]: Sync](cliConfig: CliConfig, config: Config): Stream[F, NodeConfig] =
    Stream.eval[F, NodeConfig] {
      for {
        logger <- Slf4jLogger.create[F]

        keyPair <- getKeyPair(cliConfig)

        hostName <- getHostName(cliConfig)

        portOffset = Option(cliConfig.externalPort).filter(_ != 0)
        httpPortFromArg = portOffset
        peerHttpPortFromArg = portOffset.map(_ + 1)

        httpPort <- getPort(config, httpPortFromArg, "DAG_HTTP_PORT", "http.port")
        peerHttpPort <- getPort(config, peerHttpPortFromArg, "DAG_PEER_HTTP_PORT", "http.peer-port")
        healthHttpPort <- getPort(config, None, "DAG_HEALTH_HTTP_PORT", "http.health-port")

        allocAccountBalances <- getAllocAccountBalances(cliConfig)
        _ <- logger.debug(s"Alloc: $allocAccountBalances")

        constellationConfig = config.getConfig("constellation")
        processingConfig = ProcessingConfig(maxWidth = constellationConfig.getInt("max-width"))

        whitelisting <- getWhitelisting(cliConfig)

        nodeConfig = NodeConfig(
          seeds = Seq.empty[HostPort],
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
          healthHttpPort = healthHttpPort,
          defaultTimeoutSeconds = config.getInt("default-timeout-seconds"),
          attemptDownload = !cliConfig.genesisNode,
          cliConfig = cliConfig,
          processingConfig =
            if (cliConfig.testMode) ProcessingConfig.testProcessingConfig.copy(maxWidth = 10) else processingConfig,
          dataPollingManagerOn = config.getBoolean("constellation.dataPollingManagerOn"),
          allocAccountBalances = allocAccountBalances,
          whitelisting = whitelisting,
          minRequiredSpace = constellationConfig.getInt("min-required-space")
        )
      } yield nodeConfig
    }
}
