package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.KeyPair

import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.domain.configuration.NodeConfig
import org.constellation.keytool.KeyUtils
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

  def alias: Option[String] = nodeConfig.whitelisting.get(id).flatten

  def selfAddressStr: String = id.address

  val dummyAddress: String = KeyUtils.makeKeyPair().getPublic.toId.address

  def externalHostString: String = nodeConfig.hostName

  def externalPeerHTTPPort: Int = nodeConfig.peerHttpPort

  def snapshotPath: String =
    s"tmp/${id.medium}/snapshots"

  def snapshotInfoPath: String =
    s"tmp/${id.medium}/snapshot_infos"

  def genesisObservationPath: String =
    s"tmp/${id.medium}/genesis"

  def rewardsPath: String =
    s"tmp/${id.medium}/eigen_trust"

  def updateKeyPair(kp: KeyPair): Unit =
    nodeConfig = nodeConfig.copy(primaryKeyPair = kp)

}
