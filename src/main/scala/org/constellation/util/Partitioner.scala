package org.constellation.util

import com.google.common.hash.Hashing
import org.constellation.primitives.Transaction
import org.constellation.schema.Id

/**
  * First pass at facilitator selection. Need to impl proper epidemic model. For checkpoint blocks, in lieu of min-cut,
  * choose facilitator based on who is responsible for the majority of txs in the cpb.
  */
object Partitioner {

  /*
  simple approx for exponential drop off
   */
  def log2(num: Int) = scala.math.log(num) / scala.math.log(2)

  def gossipPath(ids: List[Id], tx: Transaction) = {
    val gossipRounds = log2(ids.size).toInt
    propagationPath(ids, tx)(gossipRounds)
  }

  /*
  todo merge with selectTxFacilitator as a takeWhile and add implicit ordering for tuples
   */
  def propagationPath(ids: Seq[Id], tx: Transaction)(depth: Int): List[Id] = ids match {
    case head :: tail if depth > 0 =>
      val facilitator: Id = selectTxFacilitator(ids, tx)
      val nextFacilitator = propagationPath(ids.filterNot(_ == facilitator), tx)(depth - 1)
      facilitator :: nextFacilitator

    case _ => Nil
  }

  def numeric256(hash: Array[Byte]) = {
    val sha256 = Hashing.sha256.hashBytes(hash).asBytes()
    BigInt(sha256)
  }

  def selectTxFacilitator(ids: Seq[Id], tx: Transaction): Id = {
    val neighbors = ids.filterNot(_.hex == tx.src.address)
    val sortedNeighbors: Seq[(Id, BigInt)] =
      neighbors.map(id => (id, numeric256(id.toPublicKey.getEncoded))).sortBy(_._2)
    val (facilitatorId, _) = sortedNeighbors.minBy {
      case (id, idBi) =>
        val xorIdTx = Distance.calculate(tx.hash, id)
        val xorIdSrc = Distance.calculate(tx.src.address, id)

        xorIdTx + xorIdSrc
    }
    facilitatorId
  }
}
