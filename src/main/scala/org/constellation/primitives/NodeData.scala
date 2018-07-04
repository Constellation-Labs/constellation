package org.constellation.primitives

import java.io.File
import java.net.InetSocketAddress
import java.security.KeyPair

import org.constellation.primitives.Schema._
import org.constellation.util.Signed
import constellation._
import org.constellation.LevelDB

import scala.util.Try

trait NodeData {

  var minGenesisDistrSize: Int = 3
  @volatile var downloadMode: Boolean = true
  @volatile var downloadInProgress: Boolean = false
  var generateRandomTX: Boolean = true

  var lastConfirmationUpdateTime: Long = System.currentTimeMillis()

  @volatile implicit var keyPair: KeyPair = _

  def publicKeyHash: Int = keyPair.getPublic.hashCode()
  def id: Id = Id(keyPair.getPublic.encoded)
  def selfAddress: AddressMetaData = id.address

  @volatile var nodeState: NodeState = PendingDownload

  var externalHostString: String = _
  @volatile var externalAddress: InetSocketAddress = _
  @volatile var apiAddress: InetSocketAddress = _
  var remotes: Set[InetSocketAddress] = Set.empty[InetSocketAddress]
  def selfPeer: Signed[Peer] = Peer(id, externalAddress, Set(), apiAddress).signed()

  @volatile var db: LevelDB = _

  def tmpDirId = new File("tmp", id.medium)

  def restartDB(): Unit = {
    Try {
      db.destroy()
    }
    db = new LevelDB(new File(tmpDirId, "db"))
  }

  def updateKeyPair(kp: KeyPair): Unit = {
    keyPair = kp
    restartDB()
  }

}
