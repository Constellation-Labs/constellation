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

  var sendRandomTXV2: Boolean = false

  var minGenesisDistrSize: Int = 4
  @volatile var downloadMode: Boolean = true
  @volatile var downloadInProgress: Boolean = false
  var generateRandomTX: Boolean = true

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
/*

  @volatile var db: LevelDB = _

  def tmpDirId = new File("tmp", id.medium)

  def restartDB(): Unit = {
    Try {
      db.destroy()
    }
    db = new LevelDB(new File(tmpDirId, "db"))
  }
*/

  def updateKeyPair(kp: KeyPair): Unit = {
    keyPair = kp
    //restartDB()
  }

}
