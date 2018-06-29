package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.KeyPair

import org.constellation.primitives.Schema._
import org.constellation.util.Signed
import constellation._

trait NodeData {

  var lastConfirmationUpdateTime: Long = System.currentTimeMillis()

  @volatile implicit var keyPair: KeyPair = _

  def publicKeyHash: Int = keyPair.getPublic.hashCode()
  def id: Id = Id(keyPair.getPublic)
  def selfAddress: AddressMetaData = id.address

  @volatile var nodeState: NodeState = PendingDownload

  var externalHostString: String = _
  @volatile var externalAddress: InetSocketAddress = _
  @volatile var apiAddress: InetSocketAddress = _
  var remotes: Set[InetSocketAddress] = Set.empty[InetSocketAddress]
  def selfPeer: Signed[Peer] = Peer(id, externalAddress, Set(), apiAddress).signed()

}
