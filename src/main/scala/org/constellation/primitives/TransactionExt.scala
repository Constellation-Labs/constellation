package org.constellation.primitives

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import akka.util.Timeout
import constellation.createTransactionSafe
import org.constellation.LevelDB
import org.constellation.primitives.Schema._
import constellation._
import org.constellation.LevelDB.{DBGet, DBPut}

import scala.collection.concurrent.TrieMap
import scala.util.{Random, Try}

trait TransactionExt extends NodeData with Ledger with MetricsExt with PeerInfo {

  // Need mempool ordering by RX time -- either that or put it on a TXMetaData store
  @volatile var memPool: Set[String] = Set()
  @volatile var last100SelfSentTransactions: Seq[TransactionV1] = Seq()
  @volatile var last10000ValidTXHash: Seq[String] = Seq()

  val txSyncRequestTime : TrieMap[String, Long] = TrieMap()
  val txHashToTX : TrieMap[String, TransactionV1] = TrieMap()

  def removeTransactionFromMemory(hash: String): Unit = {
    if (txHashToTX.contains(hash)) {
      numTXRemovedFromMemory += 1
      txHashToTX.remove(hash)
    }
    txSyncRequestTime.remove(hash)
  }

  def lookupTransactionDB(hash: String): Option[TransactionV1] = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    import akka.pattern.ask
    def dbQuery = {
      dbActor.flatMap{ d => (d ? DBGet(hash)).mapTo[Option[TransactionV1]].getOpt(t=5).flatten }
    }
    dbQuery
  }

  def lookupTransactionDBFallbackBlocking(hash: String): Option[TransactionV1] = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    import akka.pattern.ask
    def dbQuery = {
      dbActor.flatMap{ d => (d ? DBGet(hash)).mapTo[Option[TransactionV1]].getOpt(t=5).flatten }
    }
    val res = txHashToTX.get(hash)
    if (res.isEmpty) dbQuery else res
  }

  def lookupTransaction(hash: String): Option[TransactionV1] = {
    // LevelDB fix here later?
    txHashToTX.get(hash)
  }

  def putTXDB(tx: TransactionV1): Unit = {
    dbActor.foreach{_ ! DBPut(tx.hash, tx)}
  }

  def storeTransaction(tx: TransactionV1): Unit = {
    txHashToTX(tx.hash) = tx
    dbActor.foreach{_ ! DBPut(tx.hash, tx)}
  }

  def createTransaction(
                         dst: String,
                         amount: Long,
                         normalized: Boolean = true,
                         src: String = id.address.address
                       ): TransactionV1 = {
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

  def acceptTransaction(tx: TransactionV1): Unit = {
    if (!last10000ValidTXHash.contains(tx.hash)) {
      val toUpdate = if (last10000ValidTXHash.size >= 10000) last10000ValidTXHash.tail
      else last10000ValidTXHash
      last10000ValidTXHash = toUpdate :+ tx.hash
      totalNumValidatedTX += 1
      tx.txData.data.updateLedger(validLedger)
      memPool -= tx.hash
    }
  }

  def updateMempool(tx: TransactionV1): Boolean = {
    val hash = tx.hash
    val txData = tx.txData.data

    val validUpdate = !memPool.contains(hash) && tx.valid && txData.ledgerValid(memPoolLedger) &&
      !last10000ValidTXHash.contains(hash)

    if (validUpdate) {
      txData.updateLedger(memPoolLedger)
      memPool += hash
    }

    validUpdate
  }

  // TODO: move into test context
  def randomTransaction(): Unit = {
    val peerAddresses = peers.map{_.data.id.address}
    val randomPeer = Random.shuffle(peerAddresses).head
    createTransaction(randomPeer.address, Random.nextInt(1000).toLong)
  }

}
