package org.constellation.util

import org.constellation.primitives.Schema.{Id, Transaction}

/** Partitioner object.
  *
  * @todo Use XOR for random partition assignment later. Needs accompanying test to validate even splits.
  * @todo define bestFacilitator.
  */
object Partitioner {

  /** Measures distance of a transaction from of a group of ids.
    *
    * @param ids ... Sequence of ID's.
    * @param tx  ... A transaction.
    * @return Min XOR distance of tx hash to any id in ids.
    */
  def minDistance(ids: Seq[Id], tx: Transaction): Id = ids.minBy { id =>
    val bi = BigInt(id.id.getEncoded)
    val bi2 = BigInt(tx.hash, 16)
    val xor = bi ^ bi2
    xor
  }

}
