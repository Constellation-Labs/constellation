package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.KeyPair
import akka.actor.ActorRef

import constellation._
import org.constellation.datastore.swaydb.SwayDBDatastore // currently unused
import org.constellation.p2p.PeerRegistrationRequest
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema._
import org.constellation.util.Signed

/** Self information access to the node. */
trait NodeData {

  // var dbActor : SwayDBDatastore = _ // tmp comment
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

  /** @return The public key hash of this as Int */
  def publicKeyHash: Int = keyPair.getPublic.hashCode()

  /** @return The Id of this as public key wrapper class. */
  def id: Id = Id(keyPair.getPublic.encoded)

  /** @return The return value of pubKeyToAddress. */
  def selfAddress: AddressMetaData = id.address

  /** @return The address of this as a string. */
  def selfAddressStr: String = selfAddress.address

  @volatile var nodeState: NodeState = NodeState.PendingDownload

  var externalHostString: String = "127.0.0.1"

  var externlPeerHTTPPort: Int = 9001

  /** @return A peer registration request. */
  def peerRegistrationRequest = PeerRegistrationRequest(externalHostString, externlPeerHTTPPort, id.b58)

  @volatile var externalAddress: Option[InetSocketAddress] = None
  @volatile var apiAddress: Option[InetSocketAddress] = None
  @volatile var tcpAddress: Option[InetSocketAddress] = None

  var remotes: Seq[InetSocketAddress] = Seq()

  /** @return Self. */
  def selfPeer: Signed[Peer] = Peer(id, externalAddress, apiAddress, remotes, externalHostString).signed()

  /** Set the key pair. */
  def updateKeyPair(kp: KeyPair): Unit = {
    keyPair = kp
  }

} // end trait NodeData

