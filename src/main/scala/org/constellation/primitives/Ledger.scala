package org.constellation.primitives

import org.constellation.primitives.Schema.{Id, TX}

import scala.collection.concurrent.TrieMap

trait Ledger extends NodeData {

  val validLedger: TrieMap[String, Long] = TrieMap()
  val memPoolLedger: TrieMap[String, Long] = TrieMap()

  def selfBalance: Option[Long] = validLedger.get(id.address.address)

  def validateTransactionBatch(txs: Set[TX], ledger: TrieMap[String, Long]): Boolean = {
    txs.toSeq.map { tx =>
      val dat = tx.txData.data
      dat.src -> dat.amount
    }.groupBy(_._1).forall {
      case (a, seq) =>
        val bal = ledger.getOrElse(a, 0L)
        bal >= seq.map {
          _._2
        }.sum
    }
  }

  def validateTXBatch(txs: Set[TX]): Boolean =
    validateTransactionBatch(txs, memPoolLedger) &&
      validateTransactionBatch(txs, validLedger)


}
