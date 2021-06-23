package org.constellation.trust

import scala.util.Random

case class TrustEdge(src: Int, dst: Int, trust: Double, isLabel: Boolean = false) {
  def other(id: Int): Int = Seq(src, dst).filterNot(_ == id).head
}

// Simple way to simulate modularity of connections / generate a topology different from random
case class TrustNode(id: Int, xCoordinate: Double, yCoordinate: Double, edges: Seq[TrustEdge] = Seq()) {

  def distance(other: TrustNode): Double =
    Math.sqrt {
      Math.pow(xCoordinate - other.xCoordinate, 2) +
        Math.pow(yCoordinate - other.yCoordinate, 2)
    } / Math.sqrt(2.0)

  def negativeEdges: Seq[TrustEdge] = edges.filter(_.trust < 0)

  def normalizedPositiveEdges(visited: Set[Int]): Map[Int, Double] = {
    val positiveSubset = positiveEdges.filterNot { e =>
      visited.contains(e.dst)
    }
    if (positiveSubset.isEmpty) Map.empty[Int, Double]
    else {
      val total = positiveSubset.map {
        _.trust
      }.sum
      val normalized = positiveSubset.map { edge =>
        edge.dst -> (edge.trust / total)
      }.toMap
      // println("Normalized sum: " + normalized.values.sum)
      normalized
    }
  }

  def positiveEdges: Seq[TrustEdge] = edges.filter(_.trust > 0)

}

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.syntax.all._

class DataGenerator[F[_]: Concurrent] {
  private final val randomEffect: Ref[F, Random] = Ref.unsafe(new Random())

  def randomEdgeLogic(distance: Double): F[Boolean] =
    for {
      random <- randomEffect.get
    } yield random.nextDouble() > distance && random.nextDouble() < 0.5

  def randomEdge(logic: Double => F[Boolean] = randomEdgeLogic)(n: TrustNode, n2: TrustNode) =
    for {
      random <- randomEffect.get
      result <- logic(n.id)
    } yield
      if (result) {
        val trustZeroToOne = random.nextDouble()
        Some(TrustEdge(n.id, n2.id, 2 * (trustZeroToOne - 0.5), isLabel = true))
      } else None

  def seedCliqueLogic(maxSeedNodeIdx: Int = 1)(id: Double): F[Boolean] =
    for {
      _ <- randomEffect.get
    } yield
      if (id <= maxSeedNodeIdx) true
      else false

  def cliqueEdge(logic: Double => F[Boolean] = seedCliqueLogic())(n: TrustNode, n2: TrustNode): F[Option[TrustEdge]] =
    for {
      random <- randomEffect.get
      result <- logic(n.distance(n2))
    } yield
      if (result) Some(TrustEdge(n.id, n2.id, 1.0, isLabel = true))
      else {
        val trustZeroToOne = random.nextDouble()
        Some(TrustEdge(n.id, n2.id, 2 * (trustZeroToOne - 0.5)))
      }

  def generateData(
    numNodes: Int = 30,
    edgeLogic: (TrustNode, TrustNode) => F[Option[TrustEdge]] = randomEdge()
  ): F[List[TrustNode]] =
    for {
      random <- randomEffect.get
      nodes = (0 until numNodes).toList.map(id => TrustNode(id, random.nextDouble(), random.nextDouble()))
      nodesWithEdges = nodes.traverse { n =>
        val edges = nodes.filterNot(_.id == n.id).traverse { n2 =>
          edgeLogic(n, n2)
        }
        edges.map(e => n.copy(edges = e.flatten))
      }
      res <- nodesWithEdges
    } yield res

  def bipartiteEdge(logic: Double => F[Boolean] = seedCliqueLogic())(n: TrustNode, n2: TrustNode) =
    for {
      result <- logic(n.id)
    } yield
      if (result) Some(TrustEdge(n.id, n2.id, 1.0, isLabel = true))
      else Some(TrustEdge(n.id, n2.id, -1.0))
}
