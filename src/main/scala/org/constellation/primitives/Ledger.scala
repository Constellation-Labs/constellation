package org.constellation.primitives

import org.constellation.primitives.Schema.{Id, Transaction}

import scala.collection.concurrent.TrieMap

trait Ledger extends NodeData {

  val validLedger: TrieMap[String, Long] = TrieMap()
  val memPoolLedger: TrieMap[String, Long] = TrieMap()

  def selfBalance: Option[Long] = validLedger.get(id.address.address)

}
