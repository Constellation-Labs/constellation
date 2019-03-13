package org.constellation

import java.util.concurrent.TimeUnit

import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.crypto.SimpleWalletLike
import org.constellation.datastore.swaydb.SwayDBDatastore
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.NodeType.NodeType
import org.constellation.primitives.Schema.{Id, NodeState, NodeType, SignedObservationEdge}
import org.constellation.primitives._

class DAO()
    extends NodeData
    with Genesis
    with EdgeDAO
    with SimpleWalletLike
    with StrictLogging {

  var initialNodeConfig : NodeConfig = _
  @volatile var nodeConfig: NodeConfig = _

  var actorMaterializer: ActorMaterializer = _

  var transactionAcceptedAfterDownload: Long = 0L
  var downloadFinishedTime: Long = System.currentTimeMillis()

  def preventLocalhostAsPeer: Boolean = !nodeConfig.allowLocalhostPeers

  def idDir = File(s"tmp/${id.medium}")

  def dbPath: File = {
    val f = File(s"tmp/${id.medium}/db")
    f.createDirectoryIfNotExists()
    f
  }

  def snapshotPath: File = {
    val f = File(s"tmp/${id.medium}/snapshots")
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

  def setNodeState(
                    nodeState_ : NodeState
                  ): Unit = {
    nodeState = nodeState_
    metrics.updateMetric("nodeState", nodeState.toString)
  }

  def peerHostPort = HostPort(nodeConfig.hostName, nodeConfig.peerHttpPort)


  def initialize(nodeConfigInit : NodeConfig = NodeConfig())(implicit materialize: ActorMaterializer = null): Unit = {
    initialNodeConfig = nodeConfigInit
    nodeConfig = nodeConfigInit
    actorMaterializer = materialize
    standardTimeout = Timeout(nodeConfig.defaultTimeoutSeconds, TimeUnit.SECONDS)

    if (nodeConfig.cliConfig.startOfflineMode) {
      nodeState = NodeState.Offline
    }

    if (nodeConfig.cliConfig.lightNode) {
      nodeType = NodeType.Light
    }

    idDir.createDirectoryIfNotExists(createParents = true)
    messageHashStore = SwayDBDatastore.duplicateCheckStore(this, "message_hash_store")
    transactionHashStore = SwayDBDatastore.duplicateCheckStore(this, "transaction_hash_store")
    checkpointHashStore = SwayDBDatastore.duplicateCheckStore(this, "checkpoint_hash_store")

  }


  def pullTips(
    allowEmptyFacilitators: Boolean = false
  ): Option[(Seq[SignedObservationEdge], Map[Id, PeerData])] = {
    threadSafeTipService.pull(allowEmptyFacilitators)(this)
  }

}
