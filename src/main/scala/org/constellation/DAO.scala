package org.constellation

import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import com.softwaremill.sttp.SttpBackend
import com.softwaremill.sttp.okhttp.OkHttpFutureBackend
import com.softwaremill.sttp.prometheus.PrometheusBackend
import com.typesafe.scalalogging.StrictLogging
import constellation._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.{
  CheckpointAcceptanceService,
  CheckpointBlockValidator,
  CheckpointParentService,
  CheckpointService
}
import org.constellation.consensus.{ConsensusManager, ConsensusRemoteSender, ConsensusScheduler, ConsensusWatcher}
import org.constellation.crypto.SimpleWalletLike
import org.constellation.datastore.swaydb.SwayDBDatastore
import org.constellation.domain.configuration.NodeConfig
import org.constellation.p2p._
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.NodeType.NodeType
import org.constellation.primitives.Schema._
import org.constellation.domain.schema.Id
import org.constellation.primitives._
import org.constellation.rollback.{RollbackAccountBalances, RollbackService}
import org.constellation.snapshot.HeightIdBasedSnapshotSelector
import org.constellation.storage._
import org.constellation.storage.external.GcpStorage
import org.constellation.storage.transactions.TransactionGossiping
import org.constellation.transaction.TransactionValidator
import org.constellation.util.{HealthChecker, HostPort, MajorityStateChooser, SnapshotWatcher}

import scala.concurrent.Future
import scala.concurrent.duration._

class DAO() extends NodeData with EdgeDAO with SimpleWalletLike with StrictLogging {

  val backend: SttpBackend[Future, Nothing] =
    PrometheusBackend[Future, Nothing](OkHttpFutureBackend()(ConstellationExecutionContext.unbounded))

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

  def snapshotHashes: Seq[String] =
    snapshotPath.list.toSeq.map { _.name }

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

    transactionService = new TransactionService[IO](this)
    transactionGossiping = new TransactionGossiping[IO](transactionService, processingConfig.txGossipingFanout, this)

    observationService = new ObservationService[IO](this)
    checkpointService = new CheckpointService[IO](
      this,
      transactionService,
      messageService,
      notificationService,
      observationService
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
        IO.contextShift(ConstellationExecutionContext.bounded),
        this
      )
    )
    addressService = new AddressService[IO]()(Concurrent(IO.ioConcurrentEffect), () => metrics)

    ipManager = IPManager[IO]()
    cluster = Cluster[IO](() => metrics, ipManager, this)

    consensusRemoteSender = new ConsensusRemoteSender[IO](IO.contextShift(ConstellationExecutionContext.bounded))

    majorityStateChooser = new MajorityStateChooser[IO]()

    val snapshotProcessor =
      new SnapshotsProcessor[IO](SnapshotsDownloader.downloadSnapshotByDistance[IO])(
        Concurrent(IO.ioConcurrentEffect),
        ioTimer.clock,
        this,
        ConstellationExecutionContext.bounded,
        IO.contextShift(ConstellationExecutionContext.bounded)
      )
    val downloadProcess = new DownloadProcess[IO](snapshotProcessor, cluster)(
      Concurrent(IO.ioConcurrentEffect),
      ioTimer,
      ioTimer.clock,
      this,
      ConstellationExecutionContext.bounded,
      IO.contextShift(ConstellationExecutionContext.bounded)
    )

    val healthChecker = new HealthChecker[IO](
      this,
      concurrentTipService,
      consensusManager,
      IO.contextShift(ConstellationExecutionContext.bounded),
      downloadProcess,
      majorityStateChooser
    )

    val snapshotSelector =
      new HeightIdBasedSnapshotSelector(this.id, processingConfig.snapshotHeightRedownloadDelayInterval)
    snapshotBroadcastService = {

      new SnapshotBroadcastService[IO](
        healthChecker,
        cluster,
        snapshotSelector,
        IO.contextShift(ConstellationExecutionContext.bounded),
        this
      )
    }

    snapshotWatcher = new SnapshotWatcher(snapshotBroadcastService)

    snapshotService = SnapshotService[IO](
      concurrentTipService,
      addressService,
      checkpointService,
      messageService,
      transactionService,
      rateLimiting,
      snapshotBroadcastService,
      consensusManager,
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
      transactionService,
      concurrentTipService,
      snapshotService,
      checkpointService,
      checkpointParentService,
      checkpointBlockValidator,
      rateLimiting,
      this
    )

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
      this,
      ConfigUtil.config,
      IO.contextShift(ConstellationExecutionContext.unbounded),
      IO.contextShift(ConstellationExecutionContext.bounded)
    )
    consensusWatcher = new ConsensusWatcher(ConfigUtil.config, consensusManager)

    transactionGenerator =
      TransactionGenerator[IO](addressService, transactionGossiping, transactionService, cluster, this)
    consensusScheduler = new ConsensusScheduler(ConfigUtil.config, consensusManager, cluster, this)

    rollbackService = new RollbackService[IO](this, new RollbackAccountBalances, snapshotService)

    cloudStorage = new GcpStorage[IO]
  }

  implicit val context: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  def peerInfo: IO[Map[Id, PeerData]] = cluster.getPeerInfo

  private def eqNodeType(nodeType: NodeType)(m: (Id, PeerData)) = m._2.peerMetadata.nodeType == nodeType
  private def eqNodeState(nodeState: NodeState)(m: (Id, PeerData)) = m._2.peerMetadata.nodeState == nodeState

  def peerInfo(nodeType: NodeType): IO[Map[Id, PeerData]] =
    peerInfo.map(_.filter(eqNodeType(nodeType)))

  def readyPeers: IO[Map[Id, PeerData]] =
    peerInfo.map(_.filter(eqNodeState(NodeState.Ready)))

  def readyPeers(nodeType: NodeType): IO[Map[Id, PeerData]] =
    readyPeers.map(_.filter(eqNodeType(nodeType)))

  def leavingPeers: IO[Map[Id, PeerData]] =
    peerInfo.map(_.filter(eqNodeState(NodeState.Leaving)))

  def terminateConsensuses(): IO[Unit] =
    consensusManager.terminateConsensuses() // TODO: wkoszycki temporary fix to check cluster stability

  def getActiveMinHeight: IO[Option[Long]] =
    consensusManager.getActiveMinHeight // TODO: wkoszycki temporary fix to check cluster stability

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
