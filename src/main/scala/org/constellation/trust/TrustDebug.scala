package org.constellation.trust
import scala.util.Random

object TrustDebug {


  case class TrustEdge(src: Int, dst: Int, trust: Double) {
    def other(id: Int): Int = Seq(src, dst).filterNot(_ == id).head
  }

  val sqrt2: Double = Math.sqrt(2)

  // Simple way to simulate modularity of connections / generate a topology different from random
  case class TrustNode(id: Int, xCoordinate: Double, yCoordinate: Double, edgeLabels: Seq[TrustEdge] = Seq()) {
    def distance(other: TrustNode): Double = Math.sqrt{
      Math.pow(xCoordinate - other.xCoordinate, 2) +
      Math.pow(yCoordinate - other.yCoordinate, 2)
    } / sqrt2
  }

  def main(args: Array[String]): Unit = {

    var nodes = (0 until 100).toList.map{ id =>
      TrustNode(id, Random.nextDouble(), Random.nextDouble())
    }

    nodes = nodes.map{ n =>
      val edges = nodes.filterNot(_.id == n.id).flatMap{ n2 =>
        if (Random.nextDouble() > n.distance(n2) && Random.nextDouble() < 0.3) {
          Some(TrustEdge(n.id, n2.id, 2*(Random.nextDouble() - 0.5)))
        } else None
      }
      println(s"Num edges ${edges.length}")
      n.copy(edgeLabels = edges)
    }

    var nodeMap = nodes.map{n => n.id -> n}.toMap


    val n = nodes.head

    println(s"Number of edges ${n.edgeLabels.size}")
//    nodes.map{ n =>

    val (positive, negative) = n.edgeLabels.partition(_.trust > 0)

    println(s"Number of positive edges ${positive.size}")
    println(s"Positive edge scores ${positive.map{_.trust}}")
    val visited = positive.map{_.dst}


    positive.foreach{ pn =>

      val degreeNormalizedTrust = pn.trust / positive.size
      val neighborNode = nodeMap(pn.dst)

      val neighborDegree = neighborNode.edgeLabels.size
      neighborNode.edgeLabels.foreach{ edge =>
        val transitiveTrust = degreeNormalizedTrust * edge.trust / neighborDegree
        println(s"Transitive edge to: ${edge.dst} with transitive trust: $transitiveTrust")
      }

      val unvisited = neighborNode.edgeLabels.filterNot(nn => visited.contains(nn.dst))

      println(s"Positive edge to: ${pn.dst} with trust: ${pn.trust} normalized: $degreeNormalizedTrust " +
        s"and transitive edges: ${neighborNode.edgeLabels.size} unvisited: ${unvisited.size}")

    }


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
