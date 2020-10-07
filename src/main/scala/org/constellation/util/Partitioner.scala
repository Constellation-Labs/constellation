package org.constellation.util

import com.google.common.hash.Hashing
import com.twitter.algebird.TopKMonoid
import org.constellation.domain.trust.TrustDataInternal
import org.constellation.schema.Id
import org.constellation.schema.transaction.Transaction

import scala.annotation.tailrec

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
    * We can rely on the weights of SelfAvoidingWalk to pre-order a cross influence space and reduce the dimensionality
    * which allows for the greedy implementation below. Relying on the previous state's nerve to re-cluster based on
    * changes in peer influence relative to its partition maintains the scale-free properties of a self similar/hierarchical
    * hausdorff cluster
    *
    * @param trustGraph output of SelfAvoidingWalk, the topology of peer influence
    * @param src
    */
  implicit class HausdorffPartitioner(trustGraph: List[TrustDataInternal])
                                        (implicit src: TrustDataInternal) {
    val k: Int = math.sqrt(trustGraph.length).toInt + 1
    lazy val nerve = hyperCover(Map(0 -> List(src)), trustGraph, k)

    //todo TrustDataInternal covariant, then this can partition hybrid channels
    implicit def partialOrder(hyperEdge: List[TrustDataInternal]): Ordering[TrustDataInternal] = CechOrdering(hyperEdge: List[TrustDataInternal])

    implicit def hausdorffMonoid(hyperEdge: List[TrustDataInternal]): TopKMonoid[TrustDataInternal] =
      new TopKMonoid[TrustDataInternal](k)(partialOrder(hyperEdge))

    @tailrec
    final def hyperCover(topology: Map[Int, List[TrustDataInternal]],
                         peers: List[TrustDataInternal],
                         k: Int): Map[Int, List[TrustDataInternal]] = {
      if (k > 0) {
        val nearestNeighbors: List[TrustDataInternal] = haussdorfCluster(topology(k), peers)(topology(k))
        val higherOrders = peers.diff(nearestNeighbors)
        val newTop = topology + (k -> nearestNeighbors)
        hyperCover(newTop, higherOrders, k - 1)
      }
      else topology
    }

    def haussdorfCluster(hyperEdge: List[TrustDataInternal], higherOrders: List[TrustDataInternal])
                        (implicit monoid: TopKMonoid[TrustDataInternal]):
    List[TrustDataInternal] = monoid.sum(higherOrders.map(l => hausdorffMonoid(hyperEdge).build(l))).items

    def rePartition(kPartite: Map[Int, List[TrustDataInternal]] = nerve)(influenceGraph: List[TrustDataInternal]):
    Map[Int, List[TrustDataInternal]] = hyperCover(kPartite, influenceGraph, k)
  }

}

object HausdorffPartitioner {
  def laplacian(hyperEdge: List[TrustDataInternal])(higherOrderNode: TrustDataInternal) = 1

  def hausdorffDistance(laplacian: Seq[TrustDataInternal], trustDataInternal: TrustDataInternal) = 1.0
}

case class CechOrdering(hyperEdge: List[TrustDataInternal]) extends Ordering[TrustDataInternal] {
  def distance(higherOrderNode: TrustDataInternal): Int = HausdorffPartitioner.laplacian(hyperEdge)(higherOrderNode)

  override def compare(x: TrustDataInternal, y: TrustDataInternal): Int =
    if (distance(x) >= distance(y)) 1
    else 0
}

