package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.KeyPair
import akka.actor.ActorRef

import constellation._
import org.constellation.p2p.PeerRegistrationRequest
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema._
import org.constellation.util.Metrics

/** Documentation. */
trait NodeData {

  // var dbActor : SwayDBDatastore = _
  var peerManager: ActorRef = _
  var metrics: Metrics = _

  @volatile var downloadMode: Boolean = true
  @volatile var downloadInProgress: Boolean = false
  @volatile var generateRandomTX: Boolean = false
  var heartbeatEnabled: Boolean = true

  var lastConfirmationUpdateTime: Long = System.currentTimeMillis()

  @volatile implicit var keyPair: KeyPair = _

  /** Documentation. */
  def publicKeyHash: Int = keyPair.getPublic.hashCode()

  /** Documentation. */
  def id: Id = keyPair.getPublic.toId

  /** Documentation. */
  def selfAddressStr: String = id.address

  @volatile var nodeState: NodeState = NodeState.PendingDownload

  var externalHostString: String = "127.0.0.1"
  var externlPeerHTTPPort: Int = 9001

  /** Documentation. */
  def peerRegistrationRequest = PeerRegistrationRequest(externalHostString, externlPeerHTTPPort, id)

  @volatile var externalAddress: Option[InetSocketAddress] = None
  @volatile var apiAddress: Option[InetSocketAddress] = None
  @volatile var tcpAddress: Option[InetSocketAddress] = None

  var remotes: Seq[InetSocketAddress] = Seq()

  /** Documentation. */
  def updateKeyPair(kp: KeyPair): Unit = {
    keyPair = kp
  }

}
