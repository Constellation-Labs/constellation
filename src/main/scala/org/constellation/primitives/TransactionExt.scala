package org.constellation.primitives

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import akka.util.Timeout
import constellation.{createTransactionSafe, _}
import org.constellation.LevelDB.{DBGet, DBPut}
import org.constellation.primitives.Schema._

import scala.collection.concurrent.TrieMap
import scala.util.Random

trait TransactionExt extends NodeData with Ledger with MetricsExt with PeerInfo {

  // Need mempool ordering by RX time -- either that or put it on a TXMetaData store
  @volatile var memPool: Set[String] = Set()
  @volatile var last100SelfSentTransactions: Seq[Transaction] = Seq()
  @volatile var last10000ValidTXHash: Seq[String] = Seq()

  val txSyncRequestTime : TrieMap[String, Long] = TrieMap()
  val txHashToTX : TrieMap[String, Transaction] = TrieMap()

  def removeTransactionFromMemory(hash: String): Unit = {
    if (txHashToTX.contains(hash)) {
      numTXRemovedFromMemory += 1
      txHashToTX.remove(hash)
    }
    txSyncRequestTime.remove(hash)
  }


  def lookupTransactionDB(hash: String): Option[Transaction] = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    import akka.pattern.ask
    def dbQuery = {
      dbActor.flatMap{ d => (d ? DBGet(hash)).mapTo[Option[Transaction]].getOpt(t=5).flatten }
    }
    dbQuery
  }


  def lookupTransactionDBFallbackBlocking(hash: String): Option[Transaction] = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    import akka.pattern.ask
    def dbQuery = {
      dbActor.flatMap{ d => (d ? DBGet(hash)).mapTo[Option[Transaction]].getOpt(t=5).flatten }
    }
    val res = txHashToTX.get(hash)
    if (res.isEmpty) dbQuery else res
  }


  def lookupTransaction(hash: String): Option[Transaction] = {
    // LevelDB fix here later?
    txHashToTX.get(hash)
  }

  def putTXDB(tx: Transaction): Unit = {
    dbActor.foreach{_ ! DBPut(tx.hash, tx)}
  }

  def storeTransaction(tx: Transaction): Unit = {
    txHashToTX(tx.hash) = tx
    dbActor.foreach{_ ! DBPut(tx.hash, tx)}
   // Try{db.put(tx)}
  }

  def createTransaction(
                         dst: String,
                         amount: Long,
                         normalized: Boolean = true,
                         src: String = id.address.address
                       ): Transaction = {
    val tx = createTransactionSafe(src, dst, amount, keyPair, normalized)
    storeTransaction(tx)
    if (last100SelfSentTransactions.size > 100) {
      last100SelfSentTransactions.tail :+ tx
    }
    last100SelfSentTransactions :+= tx
    updateMempool(tx)
    tx
  }

  def acceptTransaction(tx: Transaction): Unit = {
    if (!last10000ValidTXHash.contains(tx.hash)) {
      val toUpdate = if (last10000ValidTXHash.size >= 10000) last10000ValidTXHash.tail
      else last10000ValidTXHash
      last10000ValidTXHash = toUpdate :+ tx.hash
      totalNumValidatedTX += 1
      tx.txData.data.updateLedger(validLedger)
      memPool -= tx.hash
    }
  }

  def updateMempool(tx: Transaction): Boolean = {
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
