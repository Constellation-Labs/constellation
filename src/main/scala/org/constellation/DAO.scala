package org.constellation

import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, IO, Timer}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint._
import org.constellation.consensus._
import org.constellation.crypto.SimpleWalletLike
import org.constellation.datastore.SnapshotTrigger
import org.constellation.domain.blacklist.BlacklistedAddresses
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
import org.constellation.primitives.Schema.NodeState
import org.constellation.primitives.Schema.NodeType
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.rewards.{EigenTrust, RewardsManager}
import org.constellation.rollback.{RollbackAccountBalances, RollbackLoader, RollbackService}
import org.constellation.schema.Id
import org.constellation.storage._
import org.constellation.trust.{TrustDataPollingScheduler, TrustManager}
import org.constellation.util.{HealthChecker, HostPort, SnapshotWatcher}
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder

import scala.concurrent.Future
import scala.concurrent.duration._

class DAO() extends NodeData with EdgeDAO with SimpleWalletLike with StrictLogging {

  var apiClient: ClientInterpreter[IO] = _

  var initialNodeConfig: NodeConfig = _
  @volatile var nodeConfig: NodeConfig = _

  var node: ConstellationNode = _

  var actorMaterializer: ActorMaterializer = _

  var transactionAcceptedAfterDownload: Long = 0L
  var downloadFinishedTime: Long = System.currentTimeMillis()

  val channelStorage: ChannelStorage = ChannelStorage(this)

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
    nodeConfigInit: NodeConfig = NodeConfig()
  )(implicit materialize: ActorMaterializer = null): Unit = {
    implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

    initialNodeConfig = nodeConfigInit
    nodeConfig = nodeConfigInit
    actorMaterializer = materialize
    standardTimeout = Timeout(nodeConfig.defaultTimeoutSeconds seconds)

    if (nodeConfig.isLightNode) {
      nodeType = NodeType.Light
    }

    idDir.createDirectoryIfNotExists(createParents = true)

    implicit val ioTimer: Timer[IO] = IO.timer(ConstellationExecutionContext.unbounded)

    rateLimiting = new RateLimiting[IO]

    blacklistedAddresses = BlacklistedAddresses[IO]
    transactionChainService = TransactionChainService[IO]
    transactionService = TransactionService[IO](transactionChainService, rateLimiting, this)
    transactionGossiping = new TransactionGossiping[IO](transactionService, processingConfig.txGossipingFanout, this)
    joiningPeerValidator = JoiningPeerValidator[IO](apiClient)

    ipManager = IPManager[IO]()
    cluster = Cluster[IO](() => metrics, ipManager, joiningPeerValidator, apiClient, this)

    trustManager = TrustManager[IO](id, cluster)

    observationService = new ObservationService[IO](trustManager, this)

    val merkleService =
      new CheckpointMerkleService[IO](this, transactionService, messageService, notificationService, observationService)

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

    peerHealthCheck = PeerHealthCheck[IO](cluster, apiClient, metrics)
    peerHealthCheckWatcher = PeerHealthCheckWatcher(ConfigUtil.config, peerHealthCheck)

    snapshotTrigger = new SnapshotTrigger(
      processingConfig.snapshotTriggeringTimeSeconds
    )(this, cluster)

    redownloadPeriodicCheck = new RedownloadPeriodicCheck(
      processingConfig.redownloadPeriodicCheckTimeSeconds
    )(this)

    transactionGeneratorTrigger = new TransactionGeneratorTrigger(
      ConfigUtil.constellation.getInt("transaction.generator.randomTransactionLoopTimeSeconds"),
      this
    )

    consensusRemoteSender = new ConsensusRemoteSender[IO](
      IO.contextShift(ConstellationExecutionContext.bounded),
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
      snapshotCloudStorage = SnapshotS3Storage(
        ConfigUtil.constellation.getString("storage.aws.aws-access-key"),
        ConfigUtil.constellation.getString("storage.aws.aws-secret-key"),
        ConfigUtil.constellation.getString("storage.aws.region"),
        ConfigUtil.constellation.getString("storage.aws.bucket-name")
      )
      snapshotInfoCloudStorage = SnapshotInfoS3Storage(
        ConfigUtil.constellation.getString("storage.aws.aws-access-key"),
        ConfigUtil.constellation.getString("storage.aws.aws-secret-key"),
        ConfigUtil.constellation.getString("storage.aws.region"),
        ConfigUtil.constellation.getString("storage.aws.bucket-name")
      )
      rewardsCloudStorage = RewardsS3Storage(
        ConfigUtil.constellation.getString("storage.aws.aws-access-key"),
        ConfigUtil.constellation.getString("storage.aws.aws-secret-key"),
        ConfigUtil.constellation.getString("storage.aws.region"),
        ConfigUtil.constellation.getString("storage.aws.bucket-name")
      )
      genesisObservationCloudStorage = GenesisObservationS3Storage(
        ConfigUtil.constellation.getString("storage.aws.aws-access-key"),
        ConfigUtil.constellation.getString("storage.aws.aws-secret-key"),
        ConfigUtil.constellation.getString("storage.aws.region"),
        ConfigUtil.constellation.getString("storage.aws.bucket-name")
      )
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
      addressService = addressService
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
      this
    )

    redownloadService = RedownloadService[IO](
      ConfigUtil.constellation.getInt("snapshot.meaningfulSnapshotsCount"),
      ConfigUtil.constellation.getInt("snapshot.snapshotHeightRedownloadDelayInterval"),
      ConfigUtil.isEnabledCloudStorage,
      cluster,
      MajorityStateChooser(id),
      snapshotStorage,
      snapshotInfoStorage,
      rewardsStorage,
      snapshotService,
      checkpointAcceptanceService,
      snapshotCloudStorage,
      snapshotInfoCloudStorage,
      rewardsCloudStorage,
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
      this,
      ConfigUtil.config,
      Blocker.liftExecutionContext(ConstellationExecutionContext.unbounded),
      IO.contextShift(ConstellationExecutionContext.bounded)
    )
    consensusWatcher = new ConsensusWatcher(ConfigUtil.config, consensusManager)

    trustDataPollingScheduler = TrustDataPollingScheduler(ConfigUtil.config, trustManager, cluster, apiClient, this)

    transactionGenerator =
      TransactionGenerator[IO](addressService, transactionGossiping, transactionService, cluster, this)
    consensusScheduler = new ConsensusScheduler(ConfigUtil.config, consensusManager, cluster, this)

    rollbackService = new RollbackService[IO](
      this,
      new RollbackAccountBalances,
      snapshotService,
      rollbackLoader,
      rewardsManager,
      eigenTrust,
      snapshotStorage,
      snapshotInfoStorage,
      snapshotCloudStorage,
      snapshotInfoCloudStorage,
      rewardsStorage,
      rewardsCloudStorage,
      genesisObservationStorage,
      genesisObservationCloudStorage,
      redownloadService,
      cluster
    )
  }

  def unsafeShutdown(): Unit = {
    if (node != null) {
      implicit val ec = ConstellationExecutionContext.unbounded

//      node.peerApiBinding.flatMap(_.terminate(1.second))
//      node.apiBinding.flatMap(_.terminate(1.second))
      node.system.terminate()
    }
    if (actorMaterializer != null) actorMaterializer.shutdown()
    List(
      peerHealthCheckWatcher,
      snapshotWatcher,
      trustDataPollingScheduler,
      consensusScheduler,
      consensusWatcher,
      peerHealthCheckWatcher,
      snapshotTrigger,
      redownloadPeriodicCheck,
      transactionGeneratorTrigger
    ).filter(_ != null)
      .foreach(_.cancel().unsafeRunSync())
  }

  implicit val context: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

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
