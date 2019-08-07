package org.constellation

import java.util.concurrent.TimeUnit

import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import cats.effect.{Concurrent, ContextShift, IO}
import com.typesafe.scalalogging.StrictLogging
import constellation._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.{ConsensusManager, ConsensusRemoteSender, ConsensusScheduler, ConsensusWatcher}
import org.constellation.crypto.SimpleWalletLike
import org.constellation.datastore.swaydb.SwayDBDatastore
import org.constellation.p2p._
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.NodeType.NodeType
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.storage._
import org.constellation.storage.transactions.TransactionGossiping
import org.constellation.util.{HealthChecker, HostPort, SnapshotWatcher}

class DAO() extends NodeData with EdgeDAO with SimpleWalletLike with StrictLogging {

  var initialNodeConfig: NodeConfig = _
  @volatile var nodeConfig: NodeConfig = _

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

  implicit val unsafeLogger = Slf4jLogger.getLogger[IO]

  def peerHostPort = HostPort(nodeConfig.hostName, nodeConfig.peerHttpPort)

  def initialize(
    nodeConfigInit: NodeConfig = NodeConfig()
  )(implicit materialize: ActorMaterializer = null): Unit = {
    initialNodeConfig = nodeConfigInit
    nodeConfig = nodeConfigInit
    actorMaterializer = materialize
    standardTimeout = Timeout(nodeConfig.defaultTimeoutSeconds, TimeUnit.SECONDS)

    if (nodeConfig.isLightNode) {
      nodeType = NodeType.Light
    }

    idDir.createDirectoryIfNotExists(createParents = true)
    messageHashStore = SwayDBDatastore.duplicateCheckStore(this, "message_hash_store")
    checkpointHashStore = SwayDBDatastore.duplicateCheckStore(this, "checkpoint_hash_store")

    implicit val ioTimer = IO.timer(ConstellationExecutionContext.edge)

    rateLimiting = new RateLimiting[IO]

    transactionService = new TransactionService[IO](this)
    transactionGossiping = new TransactionGossiping[IO](transactionService, processingConfig.txGossipingFanout, this)

    concurrentTipService = new ConcurrentTipService[IO](
      processingConfig.maxActiveTipsAllowedInMemory,
      processingConfig.maxWidth,
      processingConfig.maxTipUsage,
      processingConfig.numFacilitatorPeers,
      processingConfig.minPeerTimeAddedSeconds,
      this
    )

    checkpointService = new CheckpointService[IO](
      this,
      transactionService,
      messageService,
      notificationService,
      concurrentTipService,
      rateLimiting
    )
    addressService = new AddressService[IO]()(Concurrent(ConstellationConcurrentEffect.edge), () => metrics)

    ipManager = IPManager[IO]()
    cluster = Cluster[IO](() => metrics, ipManager, this)

    consensusRemoteSender = new ConsensusRemoteSender[IO]()
    consensusManager = new ConsensusManager[IO](
      transactionService,
      concurrentTipService,
      checkpointService,
      messageService,
      consensusRemoteSender,
      cluster,
      this,
      ConfigUtil.config
    )

    consensusWatcher = new ConsensusWatcher(ConfigUtil.config, consensusManager)

    snapshotBroadcastService = {
      val snapshotProcessor =
        new SnapshotsProcessor(SnapshotsDownloader.downloadSnapshotByDistance)(
          this,
          ConstellationExecutionContext.global
        )
      val downloadProcess = new DownloadProcess(snapshotProcessor)(this, ConstellationExecutionContext.global)
      new SnapshotBroadcastService[IO](new HealthChecker[IO](this, concurrentTipService, downloadProcess),
                                       cluster,
                                       this)
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

    transactionGenerator =
      TransactionGenerator[IO](addressService, transactionGossiping, transactionService, cluster, this)
    consensusScheduler = new ConsensusScheduler(ConfigUtil.config, consensusManager, cluster, this)
  }

  implicit val context: ContextShift[IO] = ConstellationContextShift.global

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

  def readyFacilitatorsAsync: IO[Map[Id, PeerData]] =
    readyPeers(NodeType.Full).map(_.filter {
      case (_, pd) =>
        pd.peerMetadata.timeAdded < (System
          .currentTimeMillis() - processingConfig.minPeerTimeAddedSeconds * 1000)
    })
}
