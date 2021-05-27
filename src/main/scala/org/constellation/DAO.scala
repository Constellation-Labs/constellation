package org.constellation

import better.files.File
import cats.data.NonEmptyList
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, IO, Timer}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil.AWSStorageConfig
import org.constellation.checkpoint._
import org.constellation.consensus._
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.cloud.CloudService.CloudServiceEnqueue
import org.constellation.domain.cloud.{CloudStorageOld, HeightHashFileStorage}
import org.constellation.domain.cluster.{ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.genesis.GenesisStorageAlgebra
import org.constellation.domain.healthcheck.HealthCheckConsensusManager
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.domain.redownload.{
  DownloadService,
  MajorityStateChooser,
  MissingProposalFinder,
  RedownloadService,
  RedownloadStorageAlgebra
}
import org.constellation.domain.rewards.StoredRewards
import org.constellation.domain.snapshot.SnapshotStorageAlgebra
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.domain.transaction.{
  TransactionChainService,
  TransactionGossiping,
  TransactionService,
  TransactionValidator
}
import org.constellation.genesis.{Genesis, GenesisObservationLocalStorage, GenesisObservationS3Storage}
import org.constellation.gossip.checkpoint.CheckpointBlockGossipService
import org.constellation.gossip.sampling.PartitionerPeerSampling
import org.constellation.gossip.snapshot.SnapshotProposalGossipService
import org.constellation.gossip.validation.MessageValidator
import org.constellation.infrastructure.checkpointBlock.CheckpointStorageInterpreter
import org.constellation.infrastructure.cloud.{AWSStorageOld, GCPStorageOld}
import org.constellation.infrastructure.cluster.{ClusterStorageInterpreter, NodeStorageInterpreter}
import org.constellation.infrastructure.genesis.GenesisStorageInterpreter
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerHealthCheckWatcher}
import org.constellation.infrastructure.redownload.{RedownloadPeriodicCheck, RedownloadStorageInterpreter}
import org.constellation.infrastructure.rewards.{RewardsLocalStorage, RewardsS3Storage}
import org.constellation.infrastructure.snapshot.{
  SnapshotInfoLocalStorage,
  SnapshotInfoS3Storage,
  SnapshotLocalStorage,
  SnapshotS3Storage,
  SnapshotStorageInterpreter
}
import org.constellation.p2p._
import org.constellation.rewards.{EigenTrust, RewardsManager}
import org.constellation.rollback.RollbackService
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache}
import org.constellation.schema.snapshot.{SnapshotInfo, StoredSnapshot}
import org.constellation.schema.{ChannelMessage, GenesisObservation, Id, NodeState, NodeType}
import org.constellation.session.SessionTokenService
import org.constellation.snapshot.{SnapshotTrigger, SnapshotWatcher}
import org.constellation.storage._
import org.constellation.trust.{TrustDataPollingScheduler, TrustManager}
import org.constellation.util.{HealthChecker, HostPort}

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class DAO(
  boundedExecutionContext: ExecutionContext,
  unboundedExecutionContext: ExecutionContext,
  unboundedHealthExecutionContext: ExecutionContext
) extends NodeData {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(unboundedExecutionContext)

  var sessionTokenService: SessionTokenService[IO] = _

  var apiClient: ClientInterpreter[IO] = _

  var initialNodeConfig: NodeConfig = _
  @volatile var nodeConfig: NodeConfig = _

  var transactionAcceptedAfterDownload: Long = 0L
  var downloadFinishedTime: Long = System.currentTimeMillis()

  var cloudService: CloudServiceEnqueue[IO] = _
  var cloudStorage: CloudStorageOld[IO] = _

  def processingConfig: ProcessingConfig = nodeConfig.processingConfig

  implicit val ioTimer: Timer[IO] = IO.timer(unboundedExecutionContext)
  implicit val ioConcurrentEffect: ConcurrentEffect[IO] = IO.ioConcurrentEffect(contextShift)

  val genesisObservationStorage: GenesisObservationLocalStorage[IO] =
    GenesisObservationLocalStorage[IO](genesisObservationPath)
  val snapshotStorage: LocalFileStorage[IO, StoredSnapshot] = SnapshotLocalStorage(snapshotPath)
  val snapshotInfoStorage: LocalFileStorage[IO, SnapshotInfo] = SnapshotInfoLocalStorage(snapshotInfoPath)
  val rewardsStorage: LocalFileStorage[IO, StoredRewards] = RewardsLocalStorage(rewardsPath)

  genesisObservationStorage.createDirectoryIfNotExists().value.unsafeRunSync
  snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync
  snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync
  rewardsStorage.createDirectoryIfNotExists().value.unsafeRunSync

  val missingProposalFinder: MissingProposalFinder = MissingProposalFinder(
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval"),
    ConfigUtil.constellation.getLong("snapshot.missingProposalOffset"),
    ConfigUtil.constellation.getLong("snapshot.missingProposalLimit"),
    id
  )

  val checkpointStorage: CheckpointStorageAlgebra[IO] = new CheckpointStorageInterpreter[IO]()
  val snapshotServiceStorage: SnapshotStorageAlgebra[IO] = new SnapshotStorageInterpreter[IO](snapshotStorage)
  val clusterStorage: ClusterStorageAlgebra[IO] = new ClusterStorageInterpreter[IO]()
  val initialState = if (nodeConfig.cliConfig.startOfflineMode) NodeState.Offline else NodeState.PendingDownload
  val nodeStorage: NodeStorageAlgebra[IO] = new NodeStorageInterpreter[IO](initialState)
  val genesisStorage: GenesisStorageAlgebra[IO] = new GenesisStorageInterpreter[IO]()

  val redownloadStorage: RedownloadStorageAlgebra[IO] = new RedownloadStorageInterpreter[IO](
    missingProposalFinder,
    ConfigUtil.constellation.getInt("snapshot.meaningfulSnapshotsCount"),
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval"),
    keyPair
  )

  val joiningPeerValidator: JoiningPeerValidator[IO] =
    JoiningPeerValidator[IO](apiClient, Blocker.liftExecutionContext(unboundedExecutionContext))
  val blacklistedAddresses: BlacklistedAddresses[IO] = BlacklistedAddresses[IO]
  val transactionChainService: TransactionChainService[IO] = TransactionChainService[IO]
  val rateLimiting: RateLimiting[IO] = new RateLimiting[IO]
  val notificationService = new NotificationService[IO]()
  val channelService = new ChannelService[IO]()
  val recentBlockTracker = new RecentDataTracker[CheckpointCache](200)
  val threadSafeMessageMemPool = new ThreadSafeMessageMemPool()
  val addressService: AddressService[IO] = new AddressService[IO]()
  val messageValidator: MessageValidator = MessageValidator(id)
  val eigenTrust: EigenTrust[IO] = new EigenTrust[IO](id)

  val transactionService: TransactionService[IO] =
    TransactionService[IO](transactionChainService, rateLimiting, metrics)

  val transactionGossiping: TransactionGossiping[IO] =
    new TransactionGossiping[IO](transactionService, clusterStorage, processingConfig.txGossipingFanout, id)
  val transactionValidator: TransactionValidator[IO] = new TransactionValidator[IO](transactionService)

  val genesis: Genesis[IO] = new Genesis[IO](
    genesisStorage,
    transactionService,
    nodeStorage,
    nodeConfig,
    checkpointStorage,
    addressService,
    genesisObservationStorage,
    cloudService,
    metrics
  )

  val cluster: Cluster[IO] = Cluster[IO](
    joiningPeerValidator,
    apiClient,
    sessionTokenService,
    nodeStorage,
    clusterStorage,
    redownloadStorage,
    downloadService,
    eigenTrust,
    processingConfig,
    Blocker.liftExecutionContext(unboundedExecutionContext),
    id,
    alias.getOrElse("alias"),
    metrics,
    nodeConfig,
    peerHostPort,
    peersInfoPath,
    externalHostString,
    externalPeerHTTPPort,
    snapshotPath
  )

  val trustManager: TrustManager[IO] = TrustManager[IO](id, clusterStorage)

  val partitionerPeerSampling: PartitionerPeerSampling[IO] =
    PartitionerPeerSampling[IO](id, clusterStorage, trustManager)

  val observationService: ObservationService[IO] = new ObservationService[IO](trustManager, metrics)

  val trustDataPollingScheduler: TrustDataPollingScheduler = TrustDataPollingScheduler(
    ConfigUtil.config,
    trustManager,
    clusterStorage,
    apiClient,
    partitionerPeerSampling,
    unboundedExecutionContext,
    metrics
  )

  val dataResolver: DataResolver[IO] = DataResolver(
    keyPair,
    apiClient,
    clusterStorage,
    checkpointStorage,
    transactionService,
    observationService,
    Blocker.liftExecutionContext(unboundedExecutionContext)
  )

  val merkleService = new CheckpointMerkleService[IO](
    transactionService,
    notificationService,
    observationService,
    dataResolver
  )

  val snapshotProposalGossipService: SnapshotProposalGossipService[IO] =
    SnapshotProposalGossipService[IO](id, keyPair, partitionerPeerSampling, clusterStorage, apiClient, metrics)

  val checkpointBlockGossipService: CheckpointBlockGossipService[IO] =
    CheckpointBlockGossipService[IO](id, keyPair, partitionerPeerSampling, clusterStorage, apiClient, metrics)

  val checkpointBlockValidator: CheckpointBlockValidator[IO] = new CheckpointBlockValidator[IO](
    addressService,
    snapshotServiceStorage,
    checkpointStorage,
    transactionValidator,
    transactionChainService,
    metrics,
    id,
    transactionService
  )

  val checkpointService: CheckpointService[IO] = new CheckpointService[IO](
    merkleService,
    addressService,
    blacklistedAddresses,
    transactionService,
    observationService,
    snapshotServiceStorage,
    checkpointBlockValidator,
    nodeStorage,
    checkpointStorage,
    rateLimiting,
    dataResolver,
    boundedExecutionContext,
    processingConfig.maxWidth,
    processingConfig.maxTipUsage,
    processingConfig.numFacilitatorPeers,
    new FacilitatorFilter[IO](
      apiClient,
      Blocker.liftExecutionContext(unboundedExecutionContext),
      id
    ),
    id,
    metrics,
    keyPair
  )(contextShift)

  val rewardsManager: RewardsManager[IO] = new RewardsManager[IO](
    eigenTrust,
    addressService,
    id.address,
    metrics,
    clusterStorage,
    nodeStorage,
    checkpointStorage,
    id
  )

  val consensusRemoteSender: ConsensusRemoteSender[IO] = new ConsensusRemoteSender[IO](
    IO.contextShift(unboundedExecutionContext),
    observationService,
    apiClient,
    keyPair,
    Blocker.liftExecutionContext(unboundedExecutionContext)
  )

  val consensusManager: ConsensusManager[IO] = new ConsensusManager[IO](
    transactionService,
    checkpointService,
    checkpointStorage,
    observationService,
    consensusRemoteSender,
    clusterStorage,
    nodeStorage,
    apiClient,
    dataResolver,
    checkpointBlockGossipService,
    ConfigUtil.config,
    Blocker.liftExecutionContext(unboundedExecutionContext),
    IO.contextShift(boundedExecutionContext),
    metrics = metrics,
    id,
    keyPair
  )
  val snapshotService: SnapshotService[IO] = SnapshotService[IO](
    addressService,
    checkpointService,
    checkpointStorage,
    snapshotServiceStorage,
    transactionService,
    observationService,
    rateLimiting,
    consensusManager,
    trustManager,
    snapshotStorage,
    snapshotInfoStorage,
    rewardsStorage,
    eigenTrust,
    boundedExecutionContext,
    unboundedExecutionContext,
    metrics,
    processingConfig,
    id,
    cluster,
    clusterStorage
  )

  val redownloadService: RedownloadService[IO] = RedownloadService[IO](
    redownloadStorage,
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval"),
    nodeStorage,
    clusterStorage,
    MajorityStateChooser(id),
    missingProposalFinder,
    snapshotStorage,
    snapshotInfoStorage,
    snapshotService,
    cloudService,
    checkpointService,
    rewardsManager,
    apiClient,
    id,
    metrics,
    boundedExecutionContext,
    Blocker.liftExecutionContext(unboundedExecutionContext)
  )

  val downloadService: DownloadService[IO] = DownloadService[IO](
    redownloadService,
    redownloadStorage,
    nodeStorage,
    clusterStorage,
    checkpointService,
    checkpointStorage,
    apiClient,
    metrics,
    boundedExecutionContext,
    Blocker.liftExecutionContext(unboundedExecutionContext)
  )

  val consensusWatcher: ConsensusWatcher =
    new ConsensusWatcher(ConfigUtil.config, consensusManager, unboundedExecutionContext)

  val consensusScheduler: ConsensusScheduler =
    new ConsensusScheduler(ConfigUtil.config, consensusManager, nodeStorage, unboundedExecutionContext)


  val snapshotTrigger: SnapshotTrigger = new SnapshotTrigger(
    processingConfig.snapshotTriggeringTimeSeconds,
    unboundedExecutionContext
  )(cluster, nodeStorage, snapshotProposalGossipService, metrics, keyPair, redownloadStorage, snapshotService) // TODO: redownload and snapshot services

  var genesisObservationCloudStorage: NonEmptyList[GenesisObservationS3Storage[IO]] = _
  var snapshotCloudStorage: NonEmptyList[HeightHashFileStorage[IO, StoredSnapshot]] = _
  var snapshotInfoCloudStorage: NonEmptyList[HeightHashFileStorage[IO, SnapshotInfo]] = _
  var rewardsCloudStorage: HeightHashFileStorage[IO, StoredRewards] = _

  val healthChecker = new HealthChecker[IO](
    checkpointService,
    apiClient,
    Blocker.liftExecutionContext(unboundedExecutionContext),
    id,
    clusterStorage,
    processingConfig.numFacilitatorPeers
  )

  var snapshotWatcher: SnapshotWatcher = new SnapshotWatcher(healthChecker, unboundedExecutionContext)

  val rollbackService: RollbackService[IO] = new RollbackService[IO](
    genesis,
    snapshotService,
    snapshotStorage,
    snapshotInfoStorage,
    snapshotCloudStorage,
    snapshotInfoCloudStorage,
    genesisObservationCloudStorage,
    redownloadStorage,
    nodeStorage
  )

  val peerHealthCheck: PeerHealthCheck[IO] = {
    val cs = IO.contextShift(unboundedHealthExecutionContext)

    PeerHealthCheck[IO](
      clusterStorage,
      cluster,
      apiClient,
      metrics,
      Blocker.liftExecutionContext(unboundedHealthExecutionContext),
      healthHttpPort = nodeConfig.healthHttpPort.toString
    )(IO.ioConcurrentEffect(cs), IO.timer(unboundedHealthExecutionContext), cs)
  }

  val healthCheckConsensusManager: HealthCheckConsensusManager[IO] = {
    val cs = IO.contextShift(unboundedHealthExecutionContext)

    HealthCheckConsensusManager[IO](
      id,
      cluster,
      clusterStorage,
      nodeStorage,
      peerHealthCheck,
      metrics,
      apiClient,
      Blocker.liftExecutionContext(unboundedHealthExecutionContext),
      healthHttpPort = nodeConfig.healthHttpPort,
      peerHttpPort = nodeConfig.peerHttpPort
    )(IO.ioConcurrentEffect(cs), cs, IO.ioParallel(cs), IO.timer(unboundedHealthExecutionContext))
  }

  val peerHealthCheckWatcher: PeerHealthCheckWatcher =
    PeerHealthCheckWatcher(ConfigUtil.config, healthCheckConsensusManager, unboundedHealthExecutionContext)

  val redownloadPeriodicCheck: RedownloadPeriodicCheck = new RedownloadPeriodicCheck(
    processingConfig.redownloadPeriodicCheckTimeSeconds,
    unboundedExecutionContext,
    redownloadService
  )

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

  lazy val messageService: MessageService[IO] = new MessageService[IO]()

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

    implicit val ioTimer: Timer[IO] = IO.timer(unboundedExecutionContext)
    implicit val ioConcurrentEffect = IO.ioConcurrentEffect(contextShift)

    this.cloudService = cloudService

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

  }

}
