package org.constellation.primitives

import java.io.File
import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorRef
import org.constellation.primitives.Schema._
import org.constellation.util.Signed
import constellation._
import org.constellation.{LevelDB, LvlDB}

import scala.util.Try

trait NodeData {

  var dbActor : LvlDB = _
  var peerManager: ActorRef = _
  var consensus: ActorRef = _
  var metricsManager: ActorRef = _
  var edgeProcessor: ActorRef = _
  var memPoolManager: ActorRef = _

  var minGenesisDistrSize: Int = 3
  @volatile var downloadMode: Boolean = true
  @volatile var downloadInProgress: Boolean = false
  var generateRandomTX: Boolean = false

  var lastConfirmationUpdateTime: Long = System.currentTimeMillis()

  @volatile implicit var keyPair: KeyPair = _

  def publicKeyHash: Int = keyPair.getPublic.hashCode()
  def id: Id = Id(keyPair.getPublic.encoded)
  def selfAddress: AddressMetaData = id.address
  def selfAddressStr: String = selfAddress.address

  @volatile var nodeState: NodeState = PendingDownload

  var externalHostString: String = "127.0.0.1"
  @volatile var externalAddress: Option[InetSocketAddress] = None
  @volatile var apiAddress: Option[InetSocketAddress] = None
  @volatile var tcpAddress: Option[InetSocketAddress] = None

  var remotes: Seq[InetSocketAddress] = Seq()
  def selfPeer: Signed[Peer] = Peer(id, externalAddress, apiAddress, remotes, externalHostString).signed()

  def updateKeyPair(kp: KeyPair): Unit = {
    keyPair = kp
  }

}
