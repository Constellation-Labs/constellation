package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.KeyPair

import better.files.File
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.p2p.PeerRegistrationRequest
import org.constellation.primitives.Schema._
import org.constellation.util.Metrics
import org.constellation.{NodeConfig, ResourceInfo}

case class LocalNodeConfig(
  externalIP: String
)

trait NodeData {

  @volatile var nodeConfig: NodeConfig

  var metrics: Metrics = _
  var messageHashStore: swaydb.Set[String] = _
  var checkpointHashStore: swaydb.Set[String] = _

  val miscLogger = Logger("MiscLogger")

  @volatile var downloadMode: Boolean = true
  @volatile var downloadInProgress: Boolean = false
  @volatile var generateRandomTX: Boolean = false
  @volatile var formCheckpoints: Boolean = true
  @volatile var simulateEndpointTimeout: Boolean = false
  var heartbeatEnabled: Boolean = true

  var remotes: Seq[InetSocketAddress] = Seq()

  var lastConfirmationUpdateTime: Long = System.currentTimeMillis()

  def keyPair: KeyPair = nodeConfig.primaryKeyPair

  def publicKeyHash: Int = keyPair.getPublic.hashCode()

  def id: Id = keyPair.getPublic.toId

  def selfAddressStr: String = id.address

  val dummyAddress: String = KeyUtils.makeKeyPair().getPublic.toId.address

  def externalHostString: String = nodeConfig.hostName
  def externalPeerHTTPPort: Int = nodeConfig.peerHttpPort

  def peerRegistrationRequest =
    PeerRegistrationRequest(
      externalHostString,
      externalPeerHTTPPort,
      id,
      ResourceInfo(
        diskUsableBytes = new java.io.File(snapshotPath.pathAsString).getUsableSpace
      )
    )

  def snapshotPath: File =
    File(s"tmp/${id.medium}/snapshots").createDirectoryIfNotExists()

  def updateKeyPair(kp: KeyPair): Unit =
    nodeConfig = nodeConfig.copy(primaryKeyPair = kp)

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
