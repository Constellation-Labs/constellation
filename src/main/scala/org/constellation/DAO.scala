package org.constellation

import better.files.File
import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.typesafe.scalalogging.StrictLogging
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil.AWSStorageConfig
import org.constellation.checkpoint._
import org.constellation.consensus._
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.cloud.CloudService.CloudServiceEnqueue
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.domain.redownload.{DownloadService, MajorityStateChooser, RedownloadService}
import org.constellation.domain.transaction.{
  TransactionChainService,
  TransactionGossiping,
  TransactionService,
  TransactionValidator
}
import org.constellation.genesis.{GenesisObservationLocalStorage, GenesisObservationS3Storage}
import org.constellation.infrastructure.cloud.{AWSStorageOld, GCPStorageOld}
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerHealthCheckWatcher}
import org.constellation.infrastructure.redownload.RedownloadPeriodicCheck
import org.constellation.infrastructure.rewards.{RewardsLocalStorage, RewardsS3Storage}
import org.constellation.infrastructure.snapshot.{
  SnapshotInfoLocalStorage,
  SnapshotInfoS3Storage,
  SnapshotLocalStorage,
  SnapshotS3Storage
}
import org.constellation.p2p._
import org.constellation.rewards.{EigenTrust, RewardsManager}
import org.constellation.rollback.RollbackService
import org.constellation.schema.{Id, NodeState, NodeType}
import org.constellation.session.SessionTokenService
import org.constellation.snapshot.{SnapshotTrigger, SnapshotWatcher}
import org.constellation.storage._
import org.constellation.trust.{TrustDataPollingScheduler, TrustManager}
import org.constellation.util.{HealthChecker, HostPort}

class DAO() extends NodeData with EdgeDAO with StrictLogging {

  var sessionTokenService: SessionTokenService[IO] = _

  var apiClient: ClientInterpreter[IO] = _

  var initialNodeConfig: NodeConfig = _
  @volatile var nodeConfig: NodeConfig = _

  var node: ConstellationNode = _

  var transactionAcceptedAfterDownload: Long = 0L
  var downloadFinishedTime: Long = System.currentTimeMillis()

  def preventLocalhostAsPeer: Boolean = !nodeConfig.allowLocalhostPeers

  def idDir = File(s"tmp/${id.medium}")

  def dbPath: File = {
    val f = File(s"tmp/${id.medium}/db")
    f.createDirectoryIfNotExists()
    f
  }

  def peersInfoPath: File = {
    val f = File(s"tmp/${id.medium}/peers")
    f
  }

  def seedsPath: File = {
    val f = File(s"tmp/${id.medium}/seeds")
    f
  }

  @volatile var nodeType: NodeType = NodeType.Full

  lazy val messageService: MessageService[IO] = {
    implicit val daoImpl: DAO = this
    new MessageService[IO]()
  }

  def peerHostPort = HostPort(nodeConfig.hostName, nodeConfig.peerHttpPort)

  def initialize(
    nodeConfigInit: NodeConfig = NodeConfig(),
    cloudService: CloudServiceEnqueue[IO]
  ): Unit = {
    implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

    initialNodeConfig = nodeConfigInit
    nodeConfig = nodeConfigInit

    if (nodeConfig.isLightNode) {
      nodeType = NodeType.Light
    }

    idDir.createDirectoryIfNotExists(createParents = true)

    implicit val ioTimer: Timer[IO] = IO.timer(ConstellationExecutionContext.unbounded)
    implicit val ioConcurrentEffect = IO.ioConcurrentEffect(contextShift)

    rateLimiting = new RateLimiting[IO]

    this.cloudService = cloudService

    blacklistedAddresses = BlacklistedAddresses[IO]
    transactionChainService = TransactionChainService[IO]
    transactionService = TransactionService[IO](transactionChainService, rateLimiting, this)
    transactionGossiping = new TransactionGossiping[IO](transactionService, processingConfig.txGossipingFanout, this)
    joiningPeerValidator = JoiningPeerValidator[IO](apiClient)

    cluster = Cluster[IO](() => metrics, joiningPeerValidator, apiClient, sessionTokenService, this)

    trustManager = TrustManager[IO](id, cluster)

    observationService = new ObservationService[IO](trustManager, this)

    dataResolver = DataResolver(
      keyPair,
      apiClient,
      cluster,
      transactionService,
      observationService,
      () => checkpointAcceptanceService
    )

    val merkleService =
      new CheckpointMerkleService[IO](transactionService, notificationService, observationService, dataResolver)

    checkpointService = new CheckpointService[IO](
      this,
      merkleService
    )
    checkpointParentService = new CheckpointParentService(soeService, checkpointService, this)
    concurrentTipService = new ConcurrentTipService[IO](
      processingConfig.maxActiveTipsAllowedInMemory,
      processingConfig.maxWidth,
      processingConfig.maxTipUsage,
      processingConfig.numFacilitatorPeers,
      processingConfig.minPeerTimeAddedSeconds,
      checkpointParentService,
      this,
      new FacilitatorFilter[IO](
        apiClient,
        this
      )
    )
    addressService = new AddressService[IO]()

    peerHealthCheck = {
      val cs = IO.contextShift(ConstellationExecutionContext.unboundedHealth)
      val ce = IO.ioConcurrentEffect(cs)
      val timer = IO.timer(ConstellationExecutionContext.unboundedHealth)

      PeerHealthCheck[IO](cluster, apiClient, metrics)(ce, timer, cs)
    }
    peerHealthCheckWatcher = PeerHealthCheckWatcher(ConfigUtil.config, peerHealthCheck)

    snapshotTrigger = new SnapshotTrigger(
      processingConfig.snapshotTriggeringTimeSeconds
    )(this, cluster)

    redownloadPeriodicCheck = new RedownloadPeriodicCheck(
      processingConfig.redownloadPeriodicCheckTimeSeconds
    )(this)

    consensusRemoteSender = new ConsensusRemoteSender[IO](
      IO.contextShift(ConstellationExecutionContext.unbounded),
      observationService,
      apiClient,
      keyPair
    )

    snapshotStorage = SnapshotLocalStorage(snapshotPath)
    snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

    snapshotInfoStorage = SnapshotInfoLocalStorage(snapshotInfoPath)
    snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

    rewardsStorage = RewardsLocalStorage(rewardsPath)
    rewardsStorage.createDirectoryIfNotExists().value.unsafeRunSync

    genesisObservationStorage = GenesisObservationLocalStorage[IO](genesisObservationPath)
    genesisObservationStorage.createDirectoryIfNotExists().value.unsafeRunSync

    if (ConfigUtil.isEnabledAWSStorage) {
      val awsStorageConfigs = ConfigUtil.loadAWSStorageConfigs()

      snapshotCloudStorage = awsStorageConfigs.map {
        case AWSStorageConfig(accessKey, secretKey, region, bucket) =>
          SnapshotS3Storage(accessKey = accessKey, secretKey = secretKey, region = region, bucket = bucket)
      }
      snapshotInfoCloudStorage = awsStorageConfigs.map {
        case AWSStorageConfig(accessKey, secretKey, region, bucket) =>
          SnapshotInfoS3Storage(accessKey = accessKey, secretKey = secretKey, region = region, bucket = bucket)
      }
      rewardsCloudStorage = RewardsS3Storage(
        ConfigUtil.constellation.getString("storage.aws.aws-access-key"),
        ConfigUtil.constellation.getString("storage.aws.aws-secret-key"),
        ConfigUtil.constellation.getString("storage.aws.region"),
        ConfigUtil.constellation.getString("storage.aws.bucket-name")
      )
      genesisObservationCloudStorage = awsStorageConfigs.map {
        case AWSStorageConfig(accessKey, secretKey, region, bucket) =>
          GenesisObservationS3Storage(accessKey = accessKey, secretKey = secretKey, region = region, bucket = bucket)
      }
      cloudStorage = new AWSStorageOld[IO](
        ConfigUtil.constellation.getString("storage.aws.aws-access-key"),
        ConfigUtil.constellation.getString("storage.aws.aws-secret-key"),
        ConfigUtil.constellation.getString("storage.aws.region"),
        ConfigUtil.constellation.getString("storage.aws.bucket-name")
      )
    } else if (ConfigUtil.isEnabledGCPStorage) {
      cloudStorage = new GCPStorageOld[IO](
        ConfigUtil.constellation.getString("storage.gcp.bucket-name"),
        ConfigUtil.constellation.getString("storage.gcp.path-to-permission-file")
      )
    }

    eigenTrust = new EigenTrust[IO](id)
    rewardsManager = new RewardsManager[IO](
      eigenTrust = eigenTrust,
      checkpointService = checkpointService,
      addressService = addressService,
      selfAddress = id.address,
      metrics = metrics,
      cluster = cluster
    )

    snapshotService = SnapshotService[IO](
      concurrentTipService,
      cloudStorage,
      addressService,
      checkpointService,
      messageService,
      transactionService,
      observationService,
      rateLimiting,
      consensusManager,
      trustManager,
      soeService,
      snapshotStorage,
      snapshotInfoStorage,
      rewardsStorage,
      eigenTrust,
      this
    )

    transactionValidator = new TransactionValidator[IO](transactionService)
    checkpointBlockValidator = new CheckpointBlockValidator[IO](
      addressService,
      snapshotService,
      checkpointParentService,
      transactionValidator,
      this
    )

    checkpointAcceptanceService = new CheckpointAcceptanceService[IO](
      addressService,
      blacklistedAddresses,
      transactionService,
      observationService,
      concurrentTipService,
      snapshotService,
      checkpointService,
      checkpointParentService,
      checkpointBlockValidator,
      cluster,
      rateLimiting,
      dataResolver,
      this
    )

    redownloadService = RedownloadService[IO](
      ConfigUtil.constellation.getInt("snapshot.meaningfulSnapshotsCount"),
      ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval"),
      ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval"),
      ConfigUtil.isEnabledCloudStorage,
      cluster,
      MajorityStateChooser(id),
      snapshotStorage,
      snapshotInfoStorage,
      snapshotService,
      cloudService,
      checkpointAcceptanceService,
      rewardsManager,
      apiClient,
      metrics
    )

    downloadService = DownloadService[IO](redownloadService, cluster, checkpointAcceptanceService, apiClient, metrics)

    val healthChecker = new HealthChecker[IO](
      this,
      concurrentTipService,
      apiClient
    )

    snapshotWatcher = new SnapshotWatcher(healthChecker)

    consensusManager = new ConsensusManager[IO](
      transactionService,
      concurrentTipService,
      checkpointService,
      checkpointAcceptanceService,
      soeService,
      messageService,
      observationService,
      consensusRemoteSender,
      cluster,
      apiClient,
      dataResolver,
      this,
      ConfigUtil.config,
      Blocker.liftExecutionContext(ConstellationExecutionContext.unbounded),
      IO.contextShift(ConstellationExecutionContext.unbounded),
      metrics = metrics
    )
    consensusWatcher = new ConsensusWatcher(ConfigUtil.config, consensusManager)

    trustDataPollingScheduler = TrustDataPollingScheduler(ConfigUtil.config, trustManager, cluster, apiClient, this)

    consensusScheduler = new ConsensusScheduler(ConfigUtil.config, consensusManager, cluster, this)

    rollbackService = new RollbackService[IO](
      this,
      snapshotService,
      snapshotStorage,
      snapshotInfoStorage,
      snapshotCloudStorage,
      snapshotInfoCloudStorage,
      genesisObservationCloudStorage,
      redownloadService,
      cluster
    )
  }

  def unsafeShutdown(): Unit = {
    if (node != null) {
      implicit val ec = ConstellationExecutionContext.unbounded
    }

    List(
      peerHealthCheckWatcher,
      snapshotWatcher,
      trustDataPollingScheduler,
      consensusScheduler,
      consensusWatcher,
      snapshotTrigger,
      redownloadPeriodicCheck
    ).filter(_ != null)
      .foreach(_.cancel().unsafeRunSync())
  }

  def peerInfo: IO[Map[Id, PeerData]] = cluster.getPeerInfo

  private def eqNodeType(nodeType: NodeType)(m: (Id, PeerData)) = m._2.peerMetadata.nodeType == nodeType
  private def eqNodeState(nodeStates: Set[NodeState])(m: (Id, PeerData)) =
    nodeStates.contains(m._2.peerMetadata.nodeState)

  def peerInfo(nodeType: NodeType): IO[Map[Id, PeerData]] =
    peerInfo.map(_.filter(eqNodeType(nodeType)))

  def readyPeers: IO[Map[Id, PeerData]] =
    peerInfo.map(_.filter(eqNodeState(NodeState.readyStates)))

  def readyPeers(nodeType: NodeType): IO[Map[Id, PeerData]] =
    readyPeers.map(_.filter(eqNodeType(nodeType)))

  def leavingPeers: IO[Map[Id, PeerData]] =
    peerInfo.map(_.filter(eqNodeState(Set(NodeState.Leaving))))

  def terminateConsensuses(): IO[Unit] =
    consensusManager.terminateConsensuses() // TODO: wkoszycki temporary fix to check cluster stability

  def getActiveMinHeight: IO[Option[Long]] =
    consensusManager.getActiveMinHeight // TODO: wkoszycki temporary fix to check cluster stability

  // TODO: kpudlik ugly temp fix to make registerAgent mockable in real dao (as we can overwrite DAO methods only)
  def registerAgent(id: Id): IO[Unit] =
    eigenTrust.registerAgent(id)

  def readyFacilitatorsAsync: IO[Map[Id, PeerData]] =
    readyPeers(NodeType.Full).map(_.filter {
      case (_, pd) =>
        pd.peerMetadata.timeAdded < (System
          .currentTimeMillis() - processingConfig.minPeerTimeAddedSeconds * 1000)
    })

  def enableSimulateEndpointTimeout(): Unit = {
    simulateEndpointTimeout = true
    metrics.updateMetric("simulateEndpointTimeout", simulateEndpointTimeout.toString)
  }

  def disableSimulateEndpointTimeout(): Unit = {
    simulateEndpointTimeout = false
    metrics.updateMetric("simulateEndpointTimeout", simulateEndpointTimeout.toString)
  }

  def enableRandomTransactions(): Unit = {
    generateRandomTX = true
    metrics.updateMetric("generateRandomTX", generateRandomTX.toString)
  }

  def disableRandomTransactions(): Unit = {
    generateRandomTX = false
    metrics.updateMetric("generateRandomTX", generateRandomTX.toString)
  }

  def enableCheckpointFormation(): Unit = {
    formCheckpoints = true
    metrics.updateMetric("checkpointFormation", formCheckpoints.toString)
  }

  def disableCheckpointFormation(): Unit = {
    formCheckpoints = false
    metrics.updateMetric("checkpointFormation", formCheckpoints.toString)
  }
}
