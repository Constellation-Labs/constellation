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
  implicit class HausdorffPartition(trustGraph: List[TrustDataInternal])(implicit src: TrustDataInternal) {
    val k: Int = math.sqrt(trustGraph.length).toInt + 1
    lazy val nerve = hyperCover(Map(0 -> List(src)), trustGraph, k)

    implicit def hausdorffMonoid(hyperEdge: List[TrustDataInternal]): TopKMonoid[TrustDataInternal] =
      new TopKMonoid[TrustDataInternal](k)(HausdorffOrdering(hyperEdge))

    @tailrec
    final def hyperCover(
      topology: Map[Int, List[TrustDataInternal]],
      peers: List[TrustDataInternal],
      k: Int
    ): Map[Int, List[TrustDataInternal]] =
      if (k > 0) {
        val rank = this.k - k
        val nearestNeighbors = diffuse(topology(rank), peers)(hausdorffMonoid(peers))
        val higherOrders = peers.diff(nearestNeighbors)
        val newTop = topology + (rank + 1 -> nearestNeighbors)
        hyperCover(newTop, higherOrders, k - 1)
      } else topology

    /**
      * The node-influence-measure calculated in SelfAvoidingWalk is essentially a pullback across the dimensions of
      * Observations into a density that allows calculation of diffusion across hyperedges which is our calculation of
      * 'Conductance' or max-flow. See Lemma 4.1: https://arxiv.org/pdf/1804.11128.pdf
      *
      * @param hyperEdge
      * @param higherOrders
      * @return
      */
    def diffuse(hyperEdge: List[TrustDataInternal], higherOrders: List[TrustDataInternal])(
      implicit monoid: TopKMonoid[TrustDataInternal]
    ): List[TrustDataInternal] = monoid.sum(higherOrders.map(l => hausdorffMonoid(hyperEdge).build(l))).items

    def rePartition(influenceGraph: List[TrustDataInternal])(
      kPartite: Map[Int, List[TrustDataInternal]] = nerve
    ): Map[Int, List[TrustDataInternal]] = hyperCover(kPartite, influenceGraph, k)
  }

}

case class HausdorffOrdering(hyperEdge: List[TrustDataInternal]) extends Ordering[TrustDataInternal] {
  override def compare(x: TrustDataInternal, y: TrustDataInternal): Int = {
    val (xComplete, xSingle) = haussdorfDistance(x)
    val (yComplete, ySingle) = haussdorfDistance(y)
    if (xComplete <= yComplete && xSingle > ySingle) 1
    else if (xComplete == xSingle && xSingle == ySingle) 0
    else -1
  }

  /**
    * See eq 19 https://arxiv.org/pdf/0801.0748.pdf
    *
    * @param higherOrderNode
    * @return
    */
  def haussdorfDistance(higherOrderNode: TrustDataInternal): (Double, Double) =
    hyperEdge.foldLeft((0.0, 0.0)) {
      case ((curCompleteLinkage, curSingleLinkage), tdi) =>
        val (complete, single) = tdi.view.foldLeft((curCompleteLinkage, curSingleLinkage)) {
          case ((curCompleteLinkage, curSingleLinkage), (id, influenceMeasure)) =>
            val higherOrderMeasure = higherOrderNode.view.getOrElse(id, 0.0)
            (
              curCompleteLinkage + (influenceMeasure + higherOrderMeasure),
              curSingleLinkage - (influenceMeasure + higherOrderMeasure)
            )
        }
        (curCompleteLinkage + complete, curSingleLinkage + single)
    }
}
