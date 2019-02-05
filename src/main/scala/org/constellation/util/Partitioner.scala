package org.constellation.util

import com.google.common.hash.Hashing
import org.constellation.primitives.Schema.{Id, Transaction}

import scala.annotation.tailrec

object Partitioner {

  /*
  for checkpoint blocks, choose facilitator based on who is responsible for the majority of txs in the cpb.
   */
  //def selectCpFacilitator
  
  /*
  for exponential drop off
   */
  def log2(num: Int) = scala.math.log(num) / scala.math.log(2)

  def gossip(ids: Seq[Id], tx: Transaction) = {
    val gossipRounds = log2(ids.size).toInt
    val path = propagationPath(ids, tx)(gossipRounds)
  }

  /*
  this should be called once each time it is received by a node, to determine if it has been passed accouring to correct path
   */
  def propagationPath(ids: Seq[Id], tx: Transaction)(depth: Int): List[Id] = {
    if (depth > 0) {
      val facilitator: Id = selectTxFacilitator(ids, tx)
      val nextFacilitator = propagationPath(ids.filterNot(_ == facilitator), tx)(depth - 1)
      facilitator :: nextFacilitator
    }
    else Nil
  }

  def numeric256(hash: Array[Byte]) = {
    val sha256 = Hashing.sha256.hashBytes(hash).asBytes()
    BigInt(sha256)
  }

  def selectTxFacilitator(ids: Seq[Id], tx: Transaction): Id = {
    val neighbors = ids.filterNot(_.address == tx.src)
    val sortedNeighbors = neighbors.map(id => (id, numeric256(id.id.getEncoded))).sortBy(_._2)
    val (facilitatorId, _) = sortedNeighbors.minBy{ case (id, idBi) =>
      val txBi = numeric256(tx.hash.getBytes())
      val srcBi = numeric256(tx.src.address.getBytes())
      val xorIdTx = idBi ^ txBi
      val xorIdSrc = idBi ^ srcBi

      xorIdTx + xorIdSrc
    }
    facilitatorId
  }
}
