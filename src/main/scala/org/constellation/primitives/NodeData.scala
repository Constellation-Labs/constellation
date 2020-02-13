package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.KeyPair

import better.files.File
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.ResourceInfo
import org.constellation.domain.configuration.NodeConfig
import org.constellation.keytool.KeyUtils
import org.constellation.p2p.PeerRegistrationRequest
import org.constellation.schema.Id
import org.constellation.util.Metrics

case class LocalNodeConfig(
  externalIP: String
)

trait NodeData {

  @volatile var nodeConfig: NodeConfig

  var metrics: Metrics = _

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

  def peerRegistrationRequest(isSimulation: Boolean = false): PeerRegistrationRequest =
    PeerRegistrationRequest(
      externalHostString,
      externalPeerHTTPPort,
      id,
      ResourceInfo(
        diskUsableBytes = new java.io.File(snapshotPath.pathAsString).getUsableSpace
      ),
      isSimulation
    )

  def snapshotPath: File =
    File(s"tmp/${id.medium}/snapshots").createDirectoryIfNotExists()

  def snapshotInfoPath: File =
    File(s"tmp/${id.medium}/snapshot_infos").createDirectoryIfNotExists()

  def genesisObservationPath: File =
    File(s"tmp/${id.medium}/genesis/genesisObservation")

  def updateKeyPair(kp: KeyPair): Unit =
    nodeConfig = nodeConfig.copy(primaryKeyPair = kp)

}
