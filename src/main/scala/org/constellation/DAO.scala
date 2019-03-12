package org.constellation

import java.security.KeyPair

import akka.stream.ActorMaterializer
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.constellation.crypto.{KeyUtils, SimpleWalletLike}
import org.constellation.primitives.Schema.{Id, SignedObservationEdge}
import org.constellation.primitives._
import constellation._
import org.constellation.datastore.swaydb.SwayDBDatastore

class DAO(
           val nodeConfig: NodeInitializationConfig = NodeInitializationConfig()
         )
    extends NodeData
    with Genesis
    with EdgeDAO
    with SimpleWalletLike
    with StrictLogging {

  var actorMaterializer: ActorMaterializer = _

  var confirmWindow: Int = 30

  var transactionAcceptedAfterDownload: Long = 0L
  var downloadFinishedTime: Long = System.currentTimeMillis()

  var preventLocalhostAsPeer: Boolean = !nodeConfig.allowLocalhostPeers

  def idDir = File(s"tmp/${id.medium}")

  idDir.createDirectoryIfNotExists(createParents = true)

  def dbPath: File = {
    val f = File(s"tmp/${id.medium}/db")
    f.createDirectoryIfNotExists()
    f
  }

  def snapshotPath: File = {
    val f = File(s"tmp/${id.medium}/snapshots")
    f.createDirectoryIfNotExists()
    f
  }

  def snapshotHashes: Seq[String] = {
    snapshotPath.list.toSeq.map { _.name }
  }

  def peersInfoPath: File = {
    val f = File(s"tmp/${id.medium}/peers")
    f
  }

  def seedsPath: File = {
    val f = File(s"tmp/${id.medium}/seeds")
    f
  }


  messageHashStore = SwayDBDatastore.duplicateCheckStore(this, "message_hash_store")
  transactionHashStore = SwayDBDatastore.duplicateCheckStore(this, "transaction_hash_store")
  checkpointHashStore = SwayDBDatastore.duplicateCheckStore(this, "checkpoint_hash_store")


  def pullTips(
    allowEmptyFacilitators: Boolean = false
  ): Option[(Seq[SignedObservationEdge], Map[Id, PeerData])] = {
    threadSafeTipService.pull(allowEmptyFacilitators)(this)
  }

}
