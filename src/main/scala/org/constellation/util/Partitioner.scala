package org.constellation.util

import com.google.common.hash.Hashing
import org.constellation.primitives.Schema.{Id, Transaction}

object Partitioner {

  def numeric256(hash: Array[Byte]) = {
    val sha256 = Hashing.sha256.hashBytes(hash).asBytes()
    BigInt(sha256)
  }

  def selectTxFacilitator(ids: Seq[Id], tx: Transaction): Id = {
    val sortedIds = ids.map(id => (id, numeric256(id.id.getEncoded))).sortBy(_._2)
    val (facilitatorId, _) = sortedIds.minBy{ case (id, idBi) =>
      val txBi = numeric256(tx.hash.getBytes())
      val srcBi = numeric256(tx.src.address.getBytes())
      val xorIdTx = idBi ^ txBi
      val xorIdSrc = idBi ^ srcBi

      xorIdTx + xorIdSrc
    }
    facilitatorId
  }
}
