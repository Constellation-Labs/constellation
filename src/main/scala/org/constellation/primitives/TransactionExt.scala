package org.constellation.primitives

import java.security.KeyPair

import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import constellation.createTransactionSafe
import org.constellation.LevelDB
import org.constellation.primitives.Schema.{Id, Metrics, SendToAddress, TX}
import constellation._

import scala.collection.concurrent.TrieMap
import scala.util.{Random, Try}

trait TransactionExt extends NodeData with Ledger with MetricsExt with PeerInfo {

  @volatile var memPool: Set[String] = Set()
  @volatile var last100SelfSentTransactions: Seq[TX] = Seq()
  @volatile var last10000ValidTXHash: Seq[String] = Seq()

  val txSyncRequestTime : TrieMap[String, Long] = TrieMap()
  val txHashToTX : TrieMap[String, TX] = TrieMap()

  def lookupTransaction(hash: String): Option[TX] = {
    // LevelDB fix here later?
    txHashToTX.get(hash)
  }

  def storeTransaction(tx: TX): Unit = {
    txHashToTX(tx.hash) = tx
   // Try{db.put(tx)}
  }

  def createTransaction(
                         dst: String,
                         amount: Long,
                         normalized: Boolean = true,
                         src: String = id.address.address
                       ): TX = {
    val tx = createTransactionSafe(src, dst, amount, keyPair, normalized)
    storeTransaction(tx)
    if (last100SelfSentTransactions.size > 100) {
      last100SelfSentTransactions.tail :+ tx
    }
    last100SelfSentTransactions :+= tx
    updateMempool(tx)
    tx
  }

  def handleSendRequest(s: SendToAddress): StandardRoute = {
    val tx = createTransaction(s.dst, s.amount, s.normalized)
    complete(tx.prettyJson)
  }

  def acceptTransaction(tx: TX): Unit = {
    if (!last10000ValidTXHash.contains(tx.hash)) {
      val toUpdate = if (last10000ValidTXHash.size >= 10000) last10000ValidTXHash.tail
      else last10000ValidTXHash
      last10000ValidTXHash = toUpdate :+ tx.hash
      totalNumValidatedTX += 1
      tx.txData.data.updateLedger(validLedger)
      memPool -= tx.hash
    }
  }

  def updateMempool(tx: TX): Boolean = {
    val hash = tx.hash
    val txData = tx.txData.data
    val validUpdate = !memPool.contains(hash) && tx.valid && txData.ledgerValid(memPoolLedger) &&
      !last10000ValidTXHash.contains(hash)
    // logger.debug(s"Update mempool $validUpdate ${tx.valid} ${txData.ledgerValid(memPoolLedger)} ${!last1000ValidTX.contains(hash)}")
    if (validUpdate) {
      txData.updateLedger(memPoolLedger)
      memPool += hash
    }
    validUpdate
  }

  def randomTransaction(): Unit = {
    val peerAddresses = peers.map{_.data.id.address}
    val randomPeer = Random.shuffle(peerAddresses).head
    createTransaction(randomPeer.address, Random.nextInt(1000).toLong)
  }


}
