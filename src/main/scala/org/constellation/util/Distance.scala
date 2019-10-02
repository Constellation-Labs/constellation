package org.constellation.util

import com.google.common.hash.Hashing
import org.constellation.domain.schema.Id

object Distance {

  def calculate(hash: String, id: Id): BigInt =
    id.distance ^ numeric256(hash.getBytes())

  def calculate(id1: Id, id2: Id): BigInt =
    id1.distance ^ id2.distance

  private def numeric256(hash: Array[Byte]): BigInt = {
    val sha256 = Hashing.sha256.hashBytes(hash).asBytes()
    BigInt(sha256)
  }

}
