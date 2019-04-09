package org.constellation.util

import com.google.common.hash.Hashing
import org.constellation.primitives.Schema.Id

object Distance {
  def calculate(hash: String, id: Id): BigInt =
    numeric256(id.toPublicKey.getEncoded) ^ numeric256(hash.getBytes())

  def calculate(id1: Id, id2: Id): BigInt =
    numeric256(id1.toPublicKey.getEncoded) ^ numeric256(id2.toPublicKey.getEncoded)

  private def numeric256(hash: Array[Byte]): BigInt = {
    val sha256 = Hashing.sha256.hashBytes(hash).asBytes()
    BigInt(sha256)
  }

}
