package org.constellation.util

import com.google.common.hash.Hashing
import org.constellation.schema.Id
import org.constellation.schema.transaction.Transaction
import org.constellation.domain.trust.TrustDataInternal
import com.twitter.algebird.{MinPlus, MinPlusSemiring, MinPlusValue, Monoid}
import com.twitter.algebird.MinPlus.rig
import org.constellation.trust.TrustNode

import scala.collection.immutable

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
    val neighbors = ids.filterNot(_.address == tx.src.address)
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

  /**
    * todo add path field so we can dynamically take top K paths
    * Note we essentailly have two tuples for our Min-plus matrix, (src, order), (dst, score)
    * @param src
    * @param dst
    * @param order
    * @param score
    */
  case class DirectedHyperEdge(src: Id, dst: Id, order: Int, score: Double)

  /**
    * todo trustGraph Vector or Map
    * @param trustGraph
    * @param monoid
    * @param ord
    */
  implicit class ShortestPathPartitioner(trustGraph: List[TrustDataInternal])
                                        (implicit monoid: Monoid[DirectedHyperEdge], ord: Ordering[DirectedHyperEdge]){
    implicit val ring = new MinPlusSemiring

    implicit def toMinPlus(e: DirectedHyperEdge): MinPlus[DirectedHyperEdge] = MinPlusValue(e)

    //todo tail call position
    def directedEdges(startNode: TrustDataInternal = trustGraph.head, hyperEdgeOrder: Int = 1): Iterable[DirectedHyperEdge] = {
      val firstOrderLaplacian = laplacian(startNode, hyperEdgeOrder)
      val secondOrderLaplacian = trustGraph.tail.foldLeft(firstOrderLaplacian){
        case (prevLap, trustDataInternal) => prevLap ++ directedEdges(trustDataInternal, hyperEdgeOrder + 1)
      }
      firstOrderLaplacian ++ secondOrderLaplacian
    }

    def laplacian(fixedNode: TrustDataInternal = trustGraph.head, hyperEdgeOrder: Int = 1): immutable.Iterable[DirectedHyperEdge] = fixedNode.view.map {
      case (id, score) => DirectedHyperEdge(fixedNode.id, id, hyperEdgeOrder, score)
    }

    def shortestPath(maxLength: Int = trustGraph.length): MinPlus[DirectedHyperEdge] =
      directedEdges().map(toMinPlus).reduce(ring.plus)

    def shortestKPaths(maxLength: Int = trustGraph.length) = ???
  }
}

