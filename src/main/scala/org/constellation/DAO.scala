package org.constellation

import better.files.File
import cats.data.NonEmptyList
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, IO, Timer}
import constellation.PublicKeyExt
import fs2.concurrent.Queue
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil.AWSStorageConfig
import org.constellation.checkpoint._
import org.constellation.consensus._
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.cloud.CloudService.CloudServiceEnqueue
import org.constellation.domain.cloud.{CloudStorageOld, HeightHashFileStorage}
import org.constellation.domain.cluster.{BroadcastService, ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.genesis.GenesisStorageAlgebra
import org.constellation.domain.healthcheck.proposal.MissingProposalHealthCheckConsensusManager
import org.constellation.domain.healthcheck.ping.{PeerHealthCheck, PingHealthCheckConsensusManager}
import org.constellation.domain.observation.ObservationService
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
  //TransactionGossiping,
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
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
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
import org.constellation.keytool.KeyUtils
import org.constellation.p2p.DataResolver.DataResolverCheckpointsEnqueue
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
import org.constellation.util.{HealthChecker, HostPort, Metrics, MetricsUpdater}

import java.security.KeyPair
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class DAO(
  boundedExecutionContext: ExecutionContext,
  unboundedExecutionContext: ExecutionContext,
  unboundedHealthExecutionContext: ExecutionContext,
  val nodeConfig: NodeConfig,
  val cloudService: CloudServiceEnqueue[IO],
  val sessionTokenService: SessionTokenService[IO],
  val apiClient: ClientInterpreter[IO],
  val clusterStorage: ClusterStorageAlgebra[IO],
  val metrics: Metrics,
  val checkpointStorage: CheckpointStorageAlgebra[IO],
  val rateLimiting: RateLimiting[IO],
  val transactionChainService: TransactionChainService[IO],
  val transactionService: TransactionService[IO],
  val trustManager: TrustManager[IO],
  val observationService: ObservationService[IO],
  val dataResolver: DataResolver[IO],
  val checkpointsQueueInstance: DataResolverCheckpointsEnqueue[IO]
) {

  val keyPair: KeyPair = nodeConfig.primaryKeyPair
  val publicKeyHash: Int = keyPair.getPublic.hashCode()
  val id: Id = keyPair.getPublic.toId
  val alias: Option[String] = nodeConfig.whitelisting.get(id).flatten
  val selfAddressStr: String = id.address
  val externalHostString: String = nodeConfig.hostName
  val dummyAddress: String = KeyUtils.makeKeyPair().getPublic.toId.address

  def externalPeerHTTPPort: Int = nodeConfig.peerHttpPort

  def snapshotPath: String =
    s"tmp/${id.medium}/snapshots"

  def snapshotInfoPath: String =
    s"tmp/${id.medium}/snapshot_infos"

  def genesisObservationPath: String =
    s"tmp/${id.medium}/genesis"

  def rewardsPath: String =
    s"tmp/${id.medium}/eigen_trust"

  implicit val contextShift: ContextShift[IO] = IO.contextShift(unboundedExecutionContext)

  var transactionAcceptedAfterDownload: Long = 0L
  var downloadFinishedTime: Long = System.currentTimeMillis()

  var cloudStorage: CloudStorageOld[IO] = _
  var genesisObservationCloudStorage: NonEmptyList[GenesisObservationS3Storage[IO]] = _
  var snapshotCloudStorage: NonEmptyList[HeightHashFileStorage[IO, StoredSnapshot]] = _
  var snapshotInfoCloudStorage: NonEmptyList[HeightHashFileStorage[IO, SnapshotInfo]] = _
  var rewardsCloudStorage: HeightHashFileStorage[IO, StoredRewards] = _

  implicit val ioTimer: Timer[IO] = IO.timer(unboundedExecutionContext)
  implicit val ioConcurrentEffect: ConcurrentEffect[IO] = IO.ioConcurrentEffect(contextShift)

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

  def processingConfig: ProcessingConfig = nodeConfig.processingConfig

  val genesisObservationStorage: GenesisObservationLocalStorage[IO] =
    GenesisObservationLocalStorage[IO](genesisObservationPath)
  val snapshotStorage: LocalFileStorage[IO, StoredSnapshot] = SnapshotLocalStorage(snapshotPath)
  val snapshotInfoStorage: LocalFileStorage[IO, SnapshotInfo] = SnapshotInfoLocalStorage(snapshotInfoPath)
  val rewardsStorage: LocalFileStorage[IO, StoredRewards] = RewardsLocalStorage(rewardsPath)

  genesisObservationStorage.createDirectoryIfNotExists().value.unsafeRunSync
  snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync
  snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync
  rewardsStorage.createDirectoryIfNotExists().value.unsafeRunSync

  val missingProposalFinder = MissingProposalFinder(
    ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval"),
    id
  )

  val snapshotServiceStorage: SnapshotStorageAlgebra[IO] = new SnapshotStorageInterpreter[IO](snapshotStorage)
  val initialState = if (nodeConfig.cliConfig.startOfflineMode) NodeState.Offline else NodeState.PendingDownload
  val nodeStorage: NodeStorageAlgebra[IO] = new NodeStorageInterpreter[IO](initialState)
  val genesisStorage: GenesisStorageAlgebra[IO] = new GenesisStorageInterpreter[IO]()

  val redownloadStorage: RedownloadStorageAlgebra[IO] = new RedownloadStorageInterpreter[IO](
    keyPair
  )

  val metricsUpdater = new MetricsUpdater(
    metrics,
    checkpointStorage,
    snapshotServiceStorage,
    redownloadStorage,
    unboundedExecutionContext
  )

  val broadcastService: BroadcastService[IO] = new BroadcastService[IO](
    clusterStorage,
    nodeStorage,
    apiClient,
    metrics,
    id,
    Blocker.liftExecutionContext(unboundedExecutionContext)
  )

  val joiningPeerValidator: JoiningPeerValidator[IO] =
    JoiningPeerValidator[IO](apiClient, Blocker.liftExecutionContext(unboundedExecutionContext))
  val blacklistedAddresses: BlacklistedAddresses[IO] = BlacklistedAddresses[IO]

  val notificationService = new NotificationService[IO]()
  val channelService = new ChannelService[IO]()
  val recentBlockTracker = new RecentDataTracker[CheckpointCache](200)
//  val threadSafeMessageMemPool = new ThreadSafeMessageMemPool()
  val addressService: AddressService[IO] = new AddressService[IO]()
  val messageValidator: MessageValidator = MessageValidator(id)
  val eigenTrust: EigenTrust[IO] = new EigenTrust[IO](id)

//  val transactionGossiping: TransactionGossiping[IO] =
//    new TransactionGossiping[IO](transactionService, clusterStorage, processingConfig.txGossipingFanout, id)
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

  val snapshotPartitionerPeerSampling: PartitionerPeerSampling[IO] =
    PartitionerPeerSampling[IO](id, () => clusterStorage.getActiveFullPeersIds(), trustManager) // TODO: separate functions for fetching id's only? here and in next line

  val blockPartitionerPeerSampling: PartitionerPeerSampling[IO] =
    PartitionerPeerSampling[IO](id, () => clusterStorage.getActivePeersIds(), trustManager)

  val trustDataPollingScheduler: TrustDataPollingScheduler = TrustDataPollingScheduler(
    ConfigUtil.config,
    trustManager,
    clusterStorage,
    apiClient,
    List(
      snapshotPartitionerPeerSampling,
      blockPartitionerPeerSampling
    ),
    unboundedExecutionContext,
    metrics
  )

//  val merkleService = new CheckpointMerkleService[IO](
//    transactionService,
//    notificationService,
//    observationService,
//    dataResolver
//  )

  val snapshotProposalGossipService: SnapshotProposalGossipService[IO] =
    SnapshotProposalGossipService[IO](id, keyPair, snapshotPartitionerPeerSampling, clusterStorage, apiClient, metrics)

  val checkpointBlockGossipService: CheckpointBlockGossipService[IO] =
    CheckpointBlockGossipService[IO](id, keyPair, blockPartitionerPeerSampling, clusterStorage, apiClient, metrics)

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
    addressService,
    blacklistedAddresses,
    transactionService,
    observationService,
    snapshotServiceStorage,
    checkpointBlockValidator,
    nodeStorage,
    clusterStorage,
    apiClient,
    checkpointStorage,
    rateLimiting,
    dataResolver,
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
    keyPair,
    checkpointsQueueInstance
  )

  val checkpointCompare = new CheckpointCompare(checkpointService, clusterStorage, unboundedExecutionContext)

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
    keyPair,
    checkpointsQueueInstance
  )

  val snapshotService: SnapshotService[IO] = SnapshotService[IO](
    apiClient,
    clusterStorage,
    addressService,
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
    redownloadStorage,
    checkpointsQueueInstance,
    boundedExecutionContext,
    unboundedExecutionContext,
    metrics,
    processingConfig,
    id,
    keyPair,
    nodeConfig
  )

//  ConfigUtil.constellation.getInt("snapshot.meaningfulSnapshotsCount"),
//  ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval"),
//  ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval"),

  val redownloadService: RedownloadService[IO] = RedownloadService[IO](
    redownloadStorage,
    nodeStorage,
    clusterStorage,
    MajorityStateChooser(id),
    missingProposalFinder,
    snapshotStorage,
    snapshotInfoStorage,
    snapshotService,
    snapshotServiceStorage,
    cloudService,
    checkpointService,
    checkpointStorage,
    rewardsManager,
    apiClient,
    broadcastService,
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
    broadcastService,
    snapshotService,
    genesis,
    metrics,
    boundedExecutionContext,
    Blocker.liftExecutionContext(unboundedExecutionContext)
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
    observationService,
    broadcastService,
    processingConfig,
    Blocker.liftExecutionContext(unboundedExecutionContext),
    id,
    keyPair,
    alias.getOrElse("alias"),
    metrics,
    nodeConfig,
    HostPort(nodeConfig.hostName, nodeConfig.peerHttpPort),
    peersInfoPath,
    externalHostString,
    externalPeerHTTPPort,
    snapshotPath
  )

  val consensusWatcher: ConsensusWatcher =
    new ConsensusWatcher(ConfigUtil.config, consensusManager, unboundedExecutionContext)

  val consensusScheduler: ConsensusScheduler =
    new ConsensusScheduler(
      ConfigUtil.config,
      consensusManager,
      nodeStorage,
      clusterStorage,
      checkpointStorage,
      snapshotServiceStorage,
      redownloadStorage,
      unboundedExecutionContext
    )

  val checkpointAcceptanceRecalculationTrigger: CheckpointAcceptanceRecalculationTrigger =
    new CheckpointAcceptanceRecalculationTrigger(
      checkpointService,
      boundedExecutionContext
    )

  val checkpointAcceptanceTrigger: CheckpointAcceptanceTrigger = new CheckpointAcceptanceTrigger(
    nodeStorage,
    checkpointService,
    unboundedExecutionContext
  )

  val snapshotTrigger: SnapshotTrigger = new SnapshotTrigger(
    processingConfig.snapshotTriggeringTimeSeconds,
    unboundedExecutionContext
  )(
    cluster,
    clusterStorage,
    nodeStorage,
    snapshotProposalGossipService,
    metrics,
    keyPair,
    redownloadStorage,
    snapshotService,
    broadcastService
  )

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
    metrics,
    snapshotService,
    snapshotStorage,
    snapshotInfoStorage,
    snapshotCloudStorage,
    snapshotInfoCloudStorage,
    genesisObservationCloudStorage,
    redownloadStorage,
    nodeStorage
  )

  val peerHealthCheck = {
    val cs = IO.contextShift(unboundedHealthExecutionContext)

    PeerHealthCheck[IO](
      clusterStorage,
      apiClient,
      Blocker.liftExecutionContext(unboundedHealthExecutionContext),
      healthHttpPort = nodeConfig.healthHttpPort.toString
    )(IO.ioConcurrentEffect(cs), IO.timer(unboundedHealthExecutionContext), cs)
  }

  val pingHealthCheckConsensusManager = {
    val cs = IO.contextShift(unboundedHealthExecutionContext)

    PingHealthCheckConsensusManager[IO](
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

  val missingProposalHealthCheckConsensusManager = {
    val cs = IO.contextShift(unboundedHealthExecutionContext)

    MissingProposalHealthCheckConsensusManager[IO](
      id,
      cluster,
      clusterStorage,
      nodeStorage,
      redownloadStorage,
      missingProposalFinder,
      metrics,
      apiClient,
      Blocker.liftExecutionContext(unboundedHealthExecutionContext),
      healthHttpPort = nodeConfig.healthHttpPort,
      peerHttpPort = nodeConfig.peerHttpPort
    )(IO.ioConcurrentEffect(cs), cs, IO.ioParallel(cs), IO.timer(unboundedHealthExecutionContext))
  }

  val peerHealthCheckWatcher = PeerHealthCheckWatcher(
    ConfigUtil.config,
    pingHealthCheckConsensusManager,
    missingProposalHealthCheckConsensusManager,
    unboundedHealthExecutionContext
  )

  val redownloadPeriodicCheck: RedownloadPeriodicCheck = new RedownloadPeriodicCheck(
    processingConfig.redownloadPeriodicCheckTimeSeconds,
    unboundedExecutionContext,
    redownloadService,
    clusterStorage
  )

  def idDir: File = File(s"tmp/${id.medium}")

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

  def nodeType: NodeType = nodeConfig.nodeType

  lazy val messageService: MessageService[IO] = new MessageService[IO]()

  idDir.createDirectoryIfNotExists(createParents = true)
}
