package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorRef
import constellation._
import org.constellation.NodeConfig
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
  var heartbeatEnabled: Boolean = true

  var lastConfirmationUpdateTime: Long = System.currentTimeMillis()

  def keyPair: KeyPair = nodeConfig.primaryKeyPair

  def publicKeyHash: Int = keyPair.getPublic.hashCode()

  def id: Id = keyPair.getPublic.toId

  def selfAddressStr: String = id.address

  val dummyAddress: String = KeyUtils.makeKeyPair().getPublic.toId.address

  def externalHostString: String = nodeConfig.hostName
  def externlPeerHTTPPort: Int = nodeConfig.peerHttpPort

  def peerRegistrationRequest = PeerRegistrationRequest(externalHostString, externlPeerHTTPPort, id)

  var remotes: Seq[InetSocketAddress] = Seq()

  def updateKeyPair(kp: KeyPair): Unit = {
    nodeConfig = nodeConfig.copy(primaryKeyPair = kp)
  }

}
