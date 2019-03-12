package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorRef
import constellation._
import org.constellation.NodeInitializationConfig
import org.constellation.crypto.KeyUtils
import org.constellation.p2p.PeerRegistrationRequest
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.NodeType.NodeType
import org.constellation.primitives.Schema._
import org.constellation.util.Metrics


case class LocalNodeConfig(
                            externalIP: String
                          )

trait NodeData {

  val nodeConfig: NodeInitializationConfig
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

  @volatile implicit var keyPair: KeyPair = nodeConfig.primaryKeyPair

  def publicKeyHash: Int = keyPair.getPublic.hashCode()

  def id: Id = keyPair.getPublic.toId

  def selfAddressStr: String = id.address

  val dummyAddress: String = KeyUtils.makeKeyPair().getPublic.toId.address

  @volatile var nodeState: NodeState = if (nodeConfig.cliConfig.startOfflineMode) {
    NodeState.Offline
  } else NodeState.PendingDownload

  @volatile var nodeType: NodeType = if (nodeConfig.cliConfig.lightNode) {
    NodeType.Light
  } else NodeType.Full

  def setNodeState(
    nodeState_ : NodeState
  ): Unit = {
    nodeState = nodeState_
    metrics.updateMetric("nodeState", nodeState.toString)
  }

  var externalHostString: String = nodeConfig.hostName
  var externlPeerHTTPPort: Int = nodeConfig.peerHttpPort

  def peerRegistrationRequest = PeerRegistrationRequest(externalHostString, externlPeerHTTPPort, id)

  var remotes: Seq[InetSocketAddress] = Seq()

  def updateKeyPair(kp: KeyPair): Unit = {
    keyPair = kp
  }

}
