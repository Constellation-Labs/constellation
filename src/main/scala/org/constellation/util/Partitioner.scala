package org.constellation.util

import org.constellation.primitives.Schema.Id
import org.constellation.primitives.Transaction

object Partitioner {

  // TODO:  Use XOR for random partition assignment later.
  // Needs accompanying test to validate even splits

  def minDistance(ids: Seq[Id], tx: Transaction): Id = ids.minBy { id =>
    val bi = BigInt(id.toPublicKey.getEncoded)
    val bi2 = BigInt(tx.hash, 16)
    val xor = bi ^ bi2

    import com.google.common.hash.Hashing

    val test = ids.map { id =>
      val idHash = Hashing.sha256.hashBytes(id.toPublicKey.getEncoded).asBytes()
      val addressHash = Hashing.sha256.hashBytes(tx.src.address.getBytes()).asBytes()
      val idInt = BigInt(idHash)
      val addressInt = BigInt(addressHash)
      val txHash = Hashing.sha256.hashBytes(tx.hash.getBytes()).asBytes()
      val txInt = BigInt(txHash)
      (idInt ^ addressInt) + (idInt ^ txInt)
    }
  }

  // def bestFacilitator

}
