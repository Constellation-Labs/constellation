package org.constellation.trust
import scala.util.Random
import cats.implicits._

object TrustDebug {


  case class TrustEdge(src: Int, dst: Int, trust: Double) {
    def other(id: Int): Int = Seq(src, dst).filterNot(_ == id).head
  }

  val sqrt2: Double = Math.sqrt(2)

  // Simple way to simulate modularity of connections / generate a topology different from random
  case class TrustNode(id: Int, xCoordinate: Double, yCoordinate: Double, edges: Seq[TrustEdge] = Seq()) {
    def distance(other: TrustNode): Double = Math.sqrt{
      Math.pow(xCoordinate - other.xCoordinate, 2) +
      Math.pow(yCoordinate - other.yCoordinate, 2)
    } / sqrt2
  }

  // Add decay factor / normalization factor to ensure <1
  // Add slicing window to only consider highest trust scores by random factor.

  // Explore Nth order 'randomly' at each iteration ? ,
  // i.e. don't just take the normalized trust when asking peers ?
  // When calculating transitive trust should we also incorporate how that node trusts its own neighbor scores?
  // Or is that sufficient to calculate on the global update phase?

  def exploreNextNeighbor(
                           transitiveTrust: Double,
                           nextNeighborId: Int,
                           nodeMap: Map[Int, TrustNode],
                           visited: Seq[Int],
                           currentNumHops: Int = 0,
                           maxNumHops: Int = 1
                         ): Map[Int, Double] = {

    if (currentNumHops == maxNumHops) return Map.empty[Int, Double]

    assert(transitiveTrust > 0)

    val nextNeighbor = nodeMap(nextNeighborId)
    val positive = nextNeighbor.edges.filter(_.trust > 0)
    val nextVisited = visited :+ nextNeighborId

    val posTransitives = positive.flatMap{ pn =>

      val degreeNormalizedTrust = pn.trust / positive.size
      val neighbor2Node = nodeMap(pn.dst)
      val neighbor2Degree = neighbor2Node.edges.size

      val transitives = neighbor2Node.edges.filterNot(
        neighborEdge => nextVisited.contains(neighborEdge.dst)
      ).map{ edge =>

        val transitiveTrust = degreeNormalizedTrust * edge.trust / neighbor2Degree
        val exploreResult = exploreNextNeighbor(
          transitiveTrust, edge.dst, nodeMap, nextVisited, currentNumHops + 1, maxNumHops
        )

        exploreResult |+| Map(edge.dst -> transitiveTrust)
      }

      transitives
    }

    val sumTransitives = if (posTransitives.isEmpty) Map.empty[Int, Double] else posTransitives.reduce(_ |+| _)
    sumTransitives
  }

  def exploreOutwardsDebug(nodes: Seq[TrustNode], depth: Int = 1) = {

    val nodeMap = nodes.map{n => n.id -> n}.toMap

    val updatedNodes = nodes.map{ n =>


    val sumTransitives = exploreNextNeighbor(1D, n.id, nodeMap, Seq(n.id), maxNumHops = depth)
      //println(s"Number of edges ${n.edges.size}")
      //    nodes.map{ n =>

/*
      val positive = n.edges.filter(_.trust > 0)

      //println(s"Number of positive edges ${positive.size}")
      //println(s"Positive edge scores ${positive.map{_.trust}}")
      // val visited = positive.map{_.dst}


      // First order set of neighbors
      val posTransitives = positive.map{ pn =>

        val degreeNormalizedTrust = pn.trust / positive.size
        val neighborNode = nodeMap(pn.dst)
        // val unvisited = neighborNode.edges.filterNot(nn => visited.contains(nn.dst))

        //println(s"Positive edge to: ${pn.dst} with trust: ${pn.trust} normalized: $degreeNormalizedTrust " +
        //s"and transitive edges: ${neighborNode.edges.size} unvisited: ${unvisited.size}")

        val neighborDegree = neighborNode.edges.size
        /*        neighborNode.edges.foreach{ edge =>

                  val transitiveTrust = degreeNormalizedTrust * edge.trust / neighborDegree
                  println(s"Transitive edge to: ${edge.dst} with transitive trust: $transitiveTrust")
                }*/

        val transitives = neighborNode.edges.filterNot(
          neighborEdge => neighborEdge.dst == n.id || neighborEdge.dst == neighborNode.id
        ).map{ edge =>
          val transitiveTrust = degreeNormalizedTrust * edge.trust / neighborDegree

          exploreNextNeighbor(transitiveTrust, edge.dst, nodeMap, Seq(edge.src, n.id))
          edge.dst -> transitiveTrust
        }.toMap

        transitives
      }

      val sumTransitives = if (posTransitives.isEmpty) Map.empty[Int, Double] else posTransitives.reduce(_ |+| _)
*/

      println("-"*10 + " " + n.id)
      sumTransitives.toSeq.sortBy{_._1}.foreach{ case (id, trust) =>
        val original = n.edges.find{_.dst == id}
        val pctTrustDiff = original.map{o => 100*trust / o.trust }
        if (pctTrustDiff.nonEmpty) {
          println(
            s"Sum transitive id: $id trust: $trust original trust: ${original.map { _.trust }.getOrElse("NA")} " +
              s"pct change: ${pctTrustDiff.getOrElse("NA")}"
          )
        }
      }

      val updatedNode = n.copy(edges = n.edges.map{ edge =>
        edge.copy(trust = edge.trust + sumTransitives.getOrElse(edge.dst, 0D))
      } ++ sumTransitives.filterNot(n.edges.map{_.dst}.contains).map{case (id, trust) => TrustEdge(n.id, id, trust)} )

      updatedNode
    }

    updatedNodes
  }



  def main(args: Array[String]): Unit = {

    val nodes = (0 until 30).toList.map{ id =>
      TrustNode(id, Random.nextDouble(), Random.nextDouble())
    }

    val nodesWithEdges = nodes.map{ n =>
      val edges = nodes.filterNot(_.id == n.id).flatMap{ n2 =>
        if (Random.nextDouble() > n.distance(n2) && Random.nextDouble() < 0.5) {
          val trustZeroToOne = Random.nextDouble()
          Some(TrustEdge(n.id, n2.id, 2*(trustZeroToOne - 0.5)))
        } else None
      }
      println(s"Num edges ${edges.length}")
      n.copy(edges = edges)
    }

    val depthOneNodes = exploreOutwardsDebug(nodesWithEdges)


    /*

    val updates = positive.map{pn =>

      0
    }*/


  //    0
  //  }

/*
    var iteration = 0

    while (iteration < 100) {

      iteration += 1
*/

/*


    }
*/



/*

    // Exponential weighting by threshold of map distance to simulate peer topology?
    val links = nodes.flatMap{ src =>
      val dest = Seq.fill(Random.nextInt(12) + 2)()
      dest.map{ j =>
        val trust = Random.nextDouble()
        val trustPosNeg = (trust - 0.5)*2 + 0.2
        val dist = ((-100*trustPosNeg) + 130).toInt
        VizLink(i.toString, j.toString, dist, trust, trustPosNeg)
      }
    }

    val linksAdjusted = links.groupBy{v => Seq(v.source, v.target).sorted}.map(_._2.head).toSeq

    val nodeToEdgeTrust = linksAdjusted.flatMap{ v =>
      Seq(v.source, v.target).map{_ -> v}
    }.groupBy(_._1).map{
      case (k,vv) =>
        k -> vv.map{case (_, v) => v.other(k) -> v.trustPosNeg }
    }
*/



  }
}
