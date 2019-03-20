package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorRef
import better.files.File
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
  // var dbActor : SwayDBDatastore = _
  var peerManager: ActorRef = _
  var metrics: Metrics = _
  var messageHashStore: swaydb.Set[String] = _
  var transactionHashStore: swaydb.Set[String] = _
  var checkpointHashStore: swaydb.Set[String] = _

  @volatile var downloadMode: Boolean = true
  @volatile var downloadInProgress: Boolean = false
  @volatile var generateRandomTX: Boolean = false
  @volatile var formCheckpoints: Boolean = true
  var heartbeatEnabled: Boolean = true

  var lastConfirmationUpdateTime: Long = System.currentTimeMillis()

  def keyPair: KeyPair = nodeConfig.primaryKeyPair

  def publicKeyHash: Int = keyPair.getPublic.hashCode()

  def id: Id = keyPair.getPublic.toId

  def selfAddressStr: String = id.address

  val dummyAddress: String = KeyUtils.makeKeyPair().getPublic.toId.address

  def externalHostString: String = nodeConfig.hostName
  def externlPeerHTTPPort: Int = nodeConfig.peerHttpPort

  def peerRegistrationRequest =
    PeerRegistrationRequest(externalHostString,
                            externlPeerHTTPPort,
                            id,
                            ResourceInfo(
                              diskUsableBytes =
                                new java.io.File(snapshotPath.pathAsString).getUsableSpace
                            ))

  def snapshotPath: File = {
    val f = File(s"tmp/${id.medium}/snapshots")
    f.createDirectoryIfNotExists()
    f
  }
  var remotes: Seq[InetSocketAddress] = Seq()

  def updateKeyPair(kp: KeyPair): Unit = {
    nodeConfig = nodeConfig.copy(primaryKeyPair = kp)
  }

}
