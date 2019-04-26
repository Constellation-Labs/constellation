package org.constellation.trust
import scala.util.Random

case class TrustEdge(src: Int, dst: Int, trust: Double) {
  def other(id: Int): Int = Seq(src, dst).filterNot(_ == id).head
}


// Simple way to simulate modularity of connections / generate a topology different from random
case class TrustNode(id: Int, xCoordinate: Double, yCoordinate: Double, edges: Seq[TrustEdge] = Seq()) {
  def distance(other: TrustNode): Double = Math.sqrt{
    Math.pow(xCoordinate - other.xCoordinate, 2) +
      Math.pow(yCoordinate - other.yCoordinate, 2)
  } / TrustRank.sqrt2

  def positiveEdges: Seq[TrustEdge] = edges.filter(_.trust > 0)
  def normalizedPositiveEdges(visited: Set[Int]): Map[Int, Double] = {
    val positiveSubset = positiveEdges.filterNot{ e => visited.contains(e.dst)}
    if (positiveSubset.isEmpty) Map.empty[Int, Double] else {
      val total = positiveSubset.map { _.trust }.sum
      positiveSubset.map { edge =>
        edge.dst -> (edge.trust / total)
      }.toMap
    }
  }

}

object DataGeneration {

  def generateTestData(): List[TrustNode] = {

    val nodes = (0 until 30).toList.map{ id =>
      TrustNode(id, Random.nextDouble(), Random.nextDouble())
    }

    // TODO: Distance should influence trust score to simulate modularity
    val nodesWithEdges = nodes.map{ n =>
      val edges = nodes.filterNot(_.id == n.id).flatMap{ n2 =>

        val distance = n.distance(n2)
        if (Random.nextDouble() > distance && Random.nextDouble() < 0.5) {
          val trustZeroToOne = Random.nextDouble()
          Some(TrustEdge(n.id, n2.id, 2*(trustZeroToOne - 0.5)))
        } else None
      }
      println(s"Num edges ${edges.length}")
      n.copy(edges = edges)
    }
    nodesWithEdges
  }


}
