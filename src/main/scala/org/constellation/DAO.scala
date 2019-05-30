package org.constellation

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import cats.effect.IO
import cats.effect.IO.Async
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.crypto.SimpleWalletLike
import org.constellation.datastore.swaydb.SwayDBDatastore
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.NodeType.NodeType
import org.constellation.primitives.Schema.{Id, NodeState, NodeType, SignedObservationEdge}
import org.constellation.primitives._
import org.constellation.primitives.storage._
import org.constellation.util.HostPort

import scala.concurrent.Await
import scala.concurrent.duration._

class DAO() extends NodeData with Genesis with EdgeDAO with SimpleWalletLike with StrictLogging {

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

  def snapshotHashes: Seq[String] = {
    snapshotPath.list.toSeq.map { _.name }
  }

  def peersInfoPath: File = {
    val f = File(s"tmp/${id.medium}/peers")
    f
  }

  def seedsPath: File = {
    val f = File(s"tmp/${id.medium}/seeds")
    f
  }

  @volatile var nodeState: NodeState = NodeState.PendingDownload

  @volatile var nodeType: NodeType = NodeType.Full

  lazy val messageService = new MessageService()(this)

  def setNodeState(
    nodeState_ : NodeState
  ): Unit = {
    nodeState = nodeState_
    metrics.updateMetric("nodeState", nodeState.toString)
  }

  def peerHostPort = HostPort(nodeConfig.hostName, nodeConfig.peerHttpPort)

  def initialize(
    nodeConfigInit: NodeConfig = NodeConfig()
  )(implicit materialize: ActorMaterializer = null): Unit = {
    initialNodeConfig = nodeConfigInit
    nodeConfig = nodeConfigInit
    actorMaterializer = materialize
    standardTimeout = Timeout(nodeConfig.defaultTimeoutSeconds, TimeUnit.SECONDS)

    if (nodeConfig.cliConfig.startOfflineMode) {
      nodeState = NodeState.Offline
    }

    if (nodeConfig.isLightNode) {
      nodeType = NodeType.Light
    }

    idDir.createDirectoryIfNotExists(createParents = true)
    messageHashStore = SwayDBDatastore.duplicateCheckStore(this, "message_hash_store")
    checkpointHashStore = SwayDBDatastore.duplicateCheckStore(this, "checkpoint_hash_store")

    transactionService = new DefaultTransactionService(this) //, processingConfig.transactionLRUMaxSize)
    checkpointService = CheckpointService(this, processingConfig.checkpointLRUMaxSize)
    snapshotService = SnapshotService(this)
    addressService = new AddressService(processingConfig.addressLRUMaxSize)(_ => metrics)
  }

  lazy val concurrentTipService: ConcurrentTipService = new TrieBasedTipService(
    processingConfig.maxActiveTipsAllowedInMemory,
    processingConfig.maxWidth,
    processingConfig.numFacilitatorPeers,
    processingConfig.minPeerTimeAddedSeconds
  )(this)

  lazy val threadSafeSnapshotService = new ThreadSafeSnapshotService(concurrentTipService)

  def pullTips(
    readyFacilitators: Map[Id, PeerData]
  ): Option[(Seq[SignedObservationEdge], Map[Id, PeerData])] = {
    concurrentTipService.pull(readyFacilitators)(this.metrics)
  }

//  def peerInfo: Map[Id, PeerData] = {
//    // TODO: fix it to be Future
//    Await.result((peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]], 10.seconds)
//  }

  def peerInfoAsync: IO[Map[Id, PeerData]] = IO.async { cb =>
    import scala.util.{Failure, Success}

    (peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].onComplete {
      case Success(peerInfo) => cb(Right(peerInfo))
      case Failure(error) => cb(Left(error))
    }(edgeExecutionContext)
  }

  private def eqNodeType(nodeType: NodeType)(m: (Id, PeerData)) = m._2.peerMetadata.nodeType == nodeType
  private def isNodeReady(m: (Id, PeerData)) = m._2.peerMetadata.nodeState == NodeState.Ready

//  def peerInfo(nodeType: NodeType): Map[Id, PeerData] =
//    peerInfo.filter(eqNodeType(nodeType))

  def peerInfoAsync(nodeType: NodeType): IO[Map[Id, PeerData]] =
    peerInfoAsync.map(_.filter(eqNodeType(nodeType)))

//  def readyPeers: Map[Id, PeerData] =
//    peerInfo.filter(isNodeReady)

  def readyPeersAsync: IO[Map[Id, PeerData]] =
    peerInfoAsync.map(_.filter(isNodeReady))

//  def readyPeers(nodeType: NodeType): Map[Schema.Id, PeerData] =
//    peerInfo.filter(_._2.peerMetadata.nodeType == nodeType)

  def readyPeersAsync(nodeType: NodeType): IO[Map[Id, PeerData]] =
    readyPeersAsync.map(_.filter(eqNodeType(nodeType)))

//  def readyFacilitators(): Map[Id, PeerData] = readyPeers(NodeType.Full).filter {
//    case (_, pd) =>
//      pd.peerMetadata.timeAdded < (System
//        .currentTimeMillis() - processingConfig.minPeerTimeAddedSeconds * 1000)
//  }

  def readyFacilitatorsAsync: IO[Map[Id, PeerData]] =
    readyPeersAsync(NodeType.Full).map(_.filter {
      case (_, pd) =>
        pd.peerMetadata.timeAdded < (System
          .currentTimeMillis() - processingConfig.minPeerTimeAddedSeconds * 1000)
    })
}
