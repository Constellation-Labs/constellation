package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorRef
import constellation._
import org.constellation.datastore.Datastore
import org.constellation.p2p.PeerRegistrationRequest
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema._
import org.constellation.util.Signed

trait NodeData {

  var dbActor : Datastore = _
  var peerManager: ActorRef = _
  var consensus: ActorRef = _
  var metricsManager: ActorRef = _
  var edgeProcessor: ActorRef = _
  var memPoolManager: ActorRef = _
  var heartbeatActor: ActorRef = _
  var cpSigner: ActorRef = _

  @volatile var downloadMode: Boolean = true
  @volatile var downloadInProgress: Boolean = false
  @volatile var generateRandomTX: Boolean = false
  var heartbeatEnabled: Boolean = true

  var lastConfirmationUpdateTime: Long = System.currentTimeMillis()

  @volatile implicit var keyPair: KeyPair = _

  def publicKeyHash: Int = keyPair.getPublic.hashCode()
  def id: Id = Id(keyPair.getPublic.encoded)
  def selfAddress: AddressMetaData = id.address
  def selfAddressStr: String = selfAddress.address

  @volatile var nodeState: NodeState = NodeState.PendingDownload

  var externalHostString: String = "127.0.0.1"
  var externlPeerHTTPPort: Int = 9001

  def peerRegistrationRequest = PeerRegistrationRequest(externalHostString, externlPeerHTTPPort, id.b58)

  @volatile var externalAddress: Option[InetSocketAddress] = None
  @volatile var apiAddress: Option[InetSocketAddress] = None
  @volatile var tcpAddress: Option[InetSocketAddress] = None

  var remotes: Seq[InetSocketAddress] = Seq()
  def selfPeer: Signed[Peer] = Peer(id, externalAddress, apiAddress, remotes, externalHostString).signed()

  def updateKeyPair(kp: KeyPair): Unit = {
    keyPair = kp
  }

}
