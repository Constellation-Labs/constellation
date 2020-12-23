package org.constellation

import better.files.File
import cats.data.NonEmptyList
import cats.effect.{Blocker, ContextShift, IO, Timer}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil.AWSStorageConfig
import org.constellation.checkpoint._
import org.constellation.consensus._
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.cloud.CloudService.CloudServiceEnqueue
import org.constellation.domain.cloud.{CloudStorageOld, HeightHashFileStorage}
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.healthcheck.HealthCheckConsensusManager
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.domain.redownload.{DownloadService, MajorityStateChooser, RedownloadService}
import org.constellation.domain.rewards.StoredRewards
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.domain.transaction.{
  TransactionChainService,
  TransactionGossiping,
  TransactionService,
  TransactionValidator
}
import org.constellation.genesis.{GenesisObservationLocalStorage, GenesisObservationS3Storage}
import org.constellation.gossip.sampling.PartitionerPeerSampling
import org.constellation.gossip.snapshot.SnapshotProposalGossipService
import org.constellation.gossip.validation.MessageValidator
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

  def processingConfig: ProcessingConfig = nodeConfig.processingConfig

  // TODO: Put on Id keyed datastore (address? potentially) with other metadata
  val publicReputation: TrieMap[Id, Double] = TrieMap()
  val secretReputation: TrieMap[Id, Double] = TrieMap()

  val otherNodeScores: TrieMap[Id, TrieMap[Id, Double]] = TrieMap()

  var cloudService: CloudServiceEnqueue[IO] = _

  var cluster: Cluster[IO] = _
  var dataResolver: DataResolver[IO] = _
  var trustManager: TrustManager[IO] = _
  var transactionService: TransactionService[IO] = _
  var blacklistedAddresses: BlacklistedAddresses[IO] = _
  var transactionChainService: TransactionChainService[IO] = _
  var transactionGossiping: TransactionGossiping[IO] = _
  var observationService: ObservationService[IO] = _
  var checkpointService: CheckpointService[IO] = _
  var checkpointParentService: CheckpointParentService[IO] = _
  var checkpointAcceptanceService: CheckpointAcceptanceService[IO] = _

  var genesisObservationStorage: GenesisObservationLocalStorage[IO] = _
  var genesisObservationCloudStorage: NonEmptyList[GenesisObservationS3Storage[IO]] = _

  var snapshotStorage: LocalFileStorage[IO, StoredSnapshot] = _
  var snapshotCloudStorage: NonEmptyList[HeightHashFileStorage[IO, StoredSnapshot]] = _

  var snapshotInfoStorage: LocalFileStorage[IO, SnapshotInfo] = _
  var snapshotInfoCloudStorage: NonEmptyList[HeightHashFileStorage[IO, SnapshotInfo]] = _

  var rewardsStorage: LocalFileStorage[IO, StoredRewards] = _
  var rewardsCloudStorage: HeightHashFileStorage[IO, StoredRewards] = _

  var snapshotService: SnapshotService[IO] = _
  var concurrentTipService: ConcurrentTipService[IO] = _
  var checkpointBlockValidator: CheckpointBlockValidator[IO] = _
  var transactionValidator: TransactionValidator[IO] = _
  var rateLimiting: RateLimiting[IO] = _
  var addressService: AddressService[IO] = _
  var snapshotWatcher: SnapshotWatcher = _
  var rollbackService: RollbackService[IO] = _
  var cloudStorage: CloudStorageOld[IO] = _
  var redownloadService: RedownloadService[IO] = _
  var downloadService: DownloadService[IO] = _
  var peerHealthCheck: PeerHealthCheck[IO] = _
  var healthCheckConsensusManager: HealthCheckConsensusManager[IO] = _
  var peerHealthCheckWatcher: PeerHealthCheckWatcher = _
  var consensusRemoteSender: ConsensusRemoteSender[IO] = _
  var consensusManager: ConsensusManager[IO] = _
  var consensusWatcher: ConsensusWatcher = _
  var consensusScheduler: ConsensusScheduler = _
  var trustDataPollingScheduler: TrustDataPollingScheduler = _
  var eigenTrust: EigenTrust[IO] = _
  var rewardsManager: RewardsManager[IO] = _
  var joiningPeerValidator: JoiningPeerValidator[IO] = _
  var snapshotTrigger: SnapshotTrigger = _
  var redownloadPeriodicCheck: RedownloadPeriodicCheck = _
  var partitionerPeerSampling: PartitionerPeerSampling[IO] = _
  var snapshotProposalGossipService: SnapshotProposalGossipService[IO] = _
  var messageValidator: MessageValidator = _

  val notificationService = new NotificationService[IO]()
  val channelService = new ChannelService[IO]()
  val soeService = new SOEService[IO]()
  val recentBlockTracker = new RecentDataTracker[CheckpointCache](200)
  val threadSafeMessageMemPool = new ThreadSafeMessageMemPool()

  var genesisBlock: Option[CheckpointBlock] = None
  var genesisObservation: Option[GenesisObservation] = None

  def maxWidth: Int = processingConfig.maxWidth

  def maxTXInBlock: Int = processingConfig.maxTXInBlock

  def minCBSignatureThreshold: Int = processingConfig.numFacilitatorPeers

  val resolveNotifierCallbacks: TrieMap[String, Seq[CheckpointBlock]] = TrieMap()

  def pullMessages(minimumCount: Int): Option[Seq[ChannelMessage]] =
    threadSafeMessageMemPool.pull(minimumCount)

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

    implicit val ioTimer: Timer[IO] = IO.timer(unboundedExecutionContext)
    implicit val ioConcurrentEffect = IO.ioConcurrentEffect(contextShift)

    rateLimiting = new RateLimiting[IO]

    this.cloudService = cloudService

    blacklistedAddresses = BlacklistedAddresses[IO]
    transactionChainService = TransactionChainService[IO]
    transactionService = TransactionService[IO](transactionChainService, rateLimiting, this)
    transactionGossiping = new TransactionGossiping[IO](transactionService, processingConfig.txGossipingFanout, this)
    joiningPeerValidator = JoiningPeerValidator[IO](apiClient, Blocker.liftExecutionContext(unboundedExecutionContext))

    cluster = Cluster[IO](
      () => metrics,
      joiningPeerValidator,
      apiClient,
      sessionTokenService,
      this,
      Blocker.liftExecutionContext(unboundedExecutionContext)
    )

    trustManager = TrustManager[IO](id, cluster)

    observationService = new ObservationService[IO](trustManager, this)

    dataResolver = DataResolver(
      keyPair,
      apiClient,
      cluster,
      transactionService,
      observationService,
      () => checkpointAcceptanceService,
      Blocker.liftExecutionContext(unboundedExecutionContext)
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
        this,
        Blocker.liftExecutionContext(unboundedExecutionContext)
      )
    )
    addressService = new AddressService[IO]()

    peerHealthCheck = {
      val cs = IO.contextShift(unboundedHealthExecutionContext)
      val ce = IO.ioConcurrentEffect(cs)
      val timer = IO.timer(unboundedHealthExecutionContext)

      PeerHealthCheck[IO](
        cluster,
        apiClient,
        metrics,
        Blocker.liftExecutionContext(unboundedHealthExecutionContext),
        healthHttpPort = nodeConfig.healthHttpPort.toString
      )(ce, timer, cs)
    }

    healthCheckConsensusManager = {
      val cs = IO.contextShift(unboundedHealthExecutionContext)
      val ce = IO.ioConcurrentEffect(cs)
      val p = IO.ioParallel(cs)
      val timer = IO.timer(unboundedHealthExecutionContext)

      HealthCheckConsensusManager[IO](
        id,
        cluster,
        peerHealthCheck,
        metrics,
        apiClient,
        Blocker.liftExecutionContext(unboundedHealthExecutionContext),
        healthHttpPort = nodeConfig.healthHttpPort,
        peerHttpPort = nodeConfig.peerHttpPort
      )(ce, cs, p, timer)
    }

    peerHealthCheckWatcher =
      PeerHealthCheckWatcher(ConfigUtil.config, healthCheckConsensusManager, unboundedHealthExecutionContext)

    partitionerPeerSampling = PartitionerPeerSampling[IO](id, cluster, trustManager)

    messageValidator = MessageValidator(id)

    snapshotProposalGossipService =
      SnapshotProposalGossipService[IO](id, keyPair, partitionerPeerSampling, cluster, apiClient)

    snapshotTrigger = new SnapshotTrigger(
      processingConfig.snapshotTriggeringTimeSeconds,
      unboundedExecutionContext
    )(this, cluster, snapshotProposalGossipService)

    redownloadPeriodicCheck = new RedownloadPeriodicCheck(
      processingConfig.redownloadPeriodicCheckTimeSeconds,
      unboundedExecutionContext
    )(this)

    consensusRemoteSender = new ConsensusRemoteSender[IO](
      IO.contextShift(unboundedExecutionContext),
      observationService,
      apiClient,
      keyPair,
      Blocker.liftExecutionContext(unboundedExecutionContext)
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
      this,
      boundedExecutionContext,
      unboundedExecutionContext
    )

    transactionValidator = new TransactionValidator[IO](transactionService)
    checkpointBlockValidator = new CheckpointBlockValidator[IO](
      addressService,
      snapshotService,
      checkpointParentService,
      transactionValidator,
      transactionChainService,
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
      this,
      boundedExecutionContext
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
      keyPair,
      metrics,
      boundedExecutionContext,
      Blocker.liftExecutionContext(unboundedExecutionContext)
    )

    downloadService = DownloadService[IO](
      redownloadService,
      cluster,
      checkpointAcceptanceService,
      apiClient,
      metrics,
      boundedExecutionContext,
      Blocker.liftExecutionContext(unboundedExecutionContext)
    )

    val healthChecker = new HealthChecker[IO](
      this,
      concurrentTipService,
      apiClient,
      Blocker.liftExecutionContext(unboundedExecutionContext)
    )

    snapshotWatcher = new SnapshotWatcher(healthChecker, unboundedExecutionContext)

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
      Blocker.liftExecutionContext(unboundedExecutionContext),
      IO.contextShift(boundedExecutionContext),
      metrics = metrics
    )
    consensusWatcher = new ConsensusWatcher(ConfigUtil.config, consensusManager, unboundedExecutionContext)

    trustDataPollingScheduler = TrustDataPollingScheduler(
      ConfigUtil.config,
      trustManager,
      cluster,
      apiClient,
      partitionerPeerSampling,
      this,
      unboundedExecutionContext
    )

    consensusScheduler =
      new ConsensusScheduler(ConfigUtil.config, consensusManager, cluster, this, unboundedExecutionContext)

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
}
