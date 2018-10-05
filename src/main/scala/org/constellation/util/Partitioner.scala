package org.constellation.util

import org.constellation.primitives.Schema.{Id, Transaction}

object Partitioner {


  // TODO:  Use XOR for random partition assignment later.
  // Needs accompanying test to validate even splits
  def minDistance(ids: Seq[Id], tx: Transaction): Id = ids.minBy{ id =>
      val bi = BigInt(id.id.getEncoded)
      val bi2 = BigInt(tx.hash, 16)
      val xor = bi ^ bi2
      xor
  }

}
