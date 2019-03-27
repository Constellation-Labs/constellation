package org.constellation.trust
import scala.util.Random
import cats.implicits._

/**
  * UNFINISHED -- Work in progress
  * Modified version of EigenTrust
  * First difference is scores are not normalized 0 -> 1 but rather -1 to 1
  * This captures interference effects among distant networks.
  * In addition, transitive scores for negative trusted Nth neighbors are discarded,
  * as they are untrustworthy and shouldn't factor into the calculations
  *
  * This does not expect a global convergence of scores ala EigenTrust,
  * hence it cannot be run as a single decomposition
  *
  * This is not properly optimized but meant to serve as a proof of concept / comparison against EigenTrust
  * Should be changed to use matrix operations instead of directly computing sums
  *
  * Several effects are not yet complete, including weighting by near-neighbors during far-neighbor calculations
  * as well as localized eigen decompositions to capture more effects from regular EigenTrust
  *
  * Also needs partitioning steps (from eigen decomposition, see Graph Partitioning wiki example),
  * and the walker direction should be influenced by trust derivatives (i.e. max flow,)
  *
  */
object TrustRank {

  val sqrt2: Double = Math.sqrt(2)

  // Add slicing window to only consider highest trust scores by random factor.

  // When calculating transitive trust should we also incorporate how that node trusts its own neighbor scores,
  // see re-weighting comment below

  // TODO: Primary scores should always take direct precedence over non-primary transitive sums,
  //  they're mixed together here.

  // TODO: Add decay factor, adjust amplifications, order randomness by probability of transition ~ Abs(score)
  def exploreNextNeighbor(
                           transitiveTrust: Double,
                           nextNeighborId: Int,
                           nodeMap: Map[Int, TrustNode],
                           visited: Seq[Int],
                           currentNumHops: Int = 0,
                           maxNumHops: Int = 1,
                           amplificationFactor: Double = 1.0D,
                           edgeSliceCap: Int = 7,
                           transitiveSliceCap: Int = 7
                         ): Map[Int, Double] = {

    if (currentNumHops == maxNumHops) return Map.empty[Int, Double]

    assert(transitiveTrust > 0)

    val nextNeighbor = nodeMap(nextNeighborId)
    val positive = nextNeighbor.edges.filter(_.trust > 0)

    val positiveSubset = if (maxNumHops >= 3)  {
      Random.shuffle(positive).slice(0, edgeSliceCap)
    } else positive

    val nextVisited = visited :+ nextNeighborId

    // println(currentNumHops)

    // println(s"Next visited length ${nextVisited.size}")

    val posTransitives = positiveSubset.flatMap{ pn =>

      val degreeNormalizedTrust = pn.trust / positive.size
      val neighbor2Node = nodeMap(pn.dst)
      val neighbor2Degree = neighbor2Node.edges.size

      val candidateEdges = neighbor2Node.edges.filterNot(
        neighborEdge => nextVisited.contains(neighborEdge.dst)
      )

      val candidateSubset = if (maxNumHops >= 3)  {
        Random.shuffle(candidateEdges).slice(0, transitiveSliceCap)
      } else candidateEdges


      val transitives = candidateSubset.map{ edge =>

        // This needs to be re-weighted by original degree trust of this node by self (and eventually Nth sum)
        val transitiveTrust = amplificationFactor * degreeNormalizedTrust * edge.trust / neighbor2Degree

        val exploreResult = if (edge.trust <= 0) Map.empty[Int, Double] else {
          exploreNextNeighbor(
            transitiveTrust,
            edge.dst,
            nodeMap,
            nextVisited,
            currentNumHops + 1,
            maxNumHops,
            amplificationFactor
          )
        }

        // Only increase contribution of dst if we're below current hops of max depth etc?

        exploreResult |+| Map(edge.dst -> transitiveTrust)
      }

      transitives
    }

    val sumTransitives = if (posTransitives.isEmpty) Map.empty[Int, Double] else {
      posTransitives.reduce{
        (left: Map[Int, Double], right: Map[Int, Double]) =>
          left.map{ case (id, score) =>
            id -> {
              if (right.contains(id)) normalizedIncrease(score, right(id))
              else score
            }
          } ++ right.filterNot{case (k,v) => left.contains(k)}
      }
    }
    sumTransitives
  }

  def normalizedIncrease(trustScore: Double, increaseAmount: Double): Double = {
    (trustScore + increaseAmount) / (1 + trustScore*increaseAmount)
  }

  def updateNode(n: TrustNode, sumTransitives: Map[Int, Double]): TrustNode = {
    n.copy(edges = n.edges.map{ edge =>
      if (!sumTransitives.contains(edge.dst)) edge else {
        edge.copy(trust = normalizedIncrease(edge.trust, sumTransitives(edge.dst)))
      }
    } ++ sumTransitives.filterNot(n.edges.map{_.dst}.contains).map{case (id, trust) => TrustEdge(n.id, id, trust)} )
  }

  def exploreOutwards(nodes: List[TrustNode], depth: Int = 1, amplificationFactor: Double = 1.0D): List[TrustNode] = {

    val nodeMap = nodes.map{n => n.id -> n}.toMap

    val updatedNodes = nodes.map{ n =>

      val sumTransitives = exploreNextNeighbor(
        1D, n.id, nodeMap, Seq(n.id), maxNumHops = depth, amplificationFactor = amplificationFactor
      )

      println("-"*10 + " " + n.id)
      sumTransitives.toSeq.sortBy{_._1}.foreach{ case (id, trust) =>
        val original = n.edges.find{_.dst == id}
        val pctTrustDiff = original.map{o => 100*trust / o.trust }
        if (pctTrustDiff.nonEmpty) {
          println(
            s"Sum transitive id: $id trust: $trust original trust: ${original.map { _.trust }.getOrElse("NA")} " +
              s"pct change: ${pctTrustDiff.getOrElse("NA")} new trust: ${trust + original.map{_.trust}.getOrElse(0D)}"
          )
        }
      }

      println(s"Original node: $n")
      val updatedNode = updateNode(n, sumTransitives)
      println(s"Updated node: $updatedNode")
      updatedNode
    }

    updatedNodes
  }

  def debugExperimentalRunner(): Unit = {

    val nodesWithEdges = DataGeneration.generateTestData()

    // TODO: Parameter debugging, order expansion to higher number of iterations with randomized depth criteria.
    val first = exploreOutwards(nodesWithEdges, amplificationFactor = 10.0D)
    val second = exploreOutwards(first, depth = 2, amplificationFactor = 0.5)
    val third = exploreOutwards(second, depth = 3, amplificationFactor = 2D)

    val nodesAtIteration = Seq(
      nodesWithEdges,
      first,
      second,
      third
    )

    nodesWithEdges.indices.foreach{ nodeId =>

      println("-"*10 + " " + nodeId)

      nodesWithEdges.indices.foreach{ nodeIdJ =>

        val scoresFormatted = Seq.tabulate(nodesAtIteration.size){  round =>
          val nodesOfRound = nodesAtIteration(round)
          val nodeAtRound = nodesOfRound.find(_.id == nodeId).get
          val scoreAtRound = nodeAtRound.edges.find(_.dst == nodeIdJ).map{_.trust}
          val score = scoreAtRound.getOrElse(0.0)
          f"$score%2.2f"
        }.mkString(" ")

        println(s"dst: $nodeIdJ " + scoresFormatted)

      }


    }


  }
}
