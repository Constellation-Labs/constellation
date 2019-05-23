package org.constellation.trust
import scala.collection.immutable
import scala.util.Random

/**
  * https://en.wikipedia.org/wiki/Node_influence_metric
  * https://en.wikipedia.org/wiki/Self-avoiding_walk
  *
  */
object SelfAvoidingWalk {

  final def sample[A](dist: Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item  // return so that we don't have to search through the whole distribution
    }
    println(dist)
    sys.error(f"this should never happen")  // needed so it will compile
  }


  // TODO: Make this iterative (simpler, avoid stack depth issue) and memoize visited up to N (small)
  def walk(
            selfId: Int,
            currentId: Int,
            nodeMap: Map[Int, TrustNode],
            totalPathLength: Int,
            currentPathLength: Int,
            visited: Set[Int],
            currentTrust: Double
          ): (Int, Double) = {
    if (totalPathLength == currentPathLength) {
      currentId -> currentTrust
    } else {

      val n1 = nodeMap(currentId)

      // TODO: Visited should have a 'direction' associated to bias the walk not just in terms of trust
      // but also trust derivatives in order to move 'outward' as effectively as possible (to discourage loop formation)
      // otherwise the path length may not matter as the walks will get trapped in the same neighborhood
      // Essentially need topo information from something else processing total edge map

      val visitedNext = visited + currentId

      val normalEdges = n1.normalizedPositiveEdges(visitedNext)

      if (normalEdges.isEmpty) {
        currentId -> currentTrust
      } else {


        val transitionDst = sample(normalEdges)

        // Ignore paths that distrust self (maybe consider ignoring paths that distrust immediate
        // neighbors as well? This is where Jaccard distance is important
        // We need to discard walks where large distance exists from previous
        // (i.e. discard information from distant nodes if they distrust nearby nodes that you trust in general)
        if (nodeMap(transitionDst).edges.exists(edge => edge.trust < 0 && edge.dst == selfId)) {
          currentId -> currentTrust
        } else {

          val transitionTrust = normalEdges(transitionDst)

          val productTrust = currentTrust * transitionTrust

          walk(
            selfId,
            transitionDst,
            nodeMap,
            totalPathLength,
            currentPathLength + 1,
            visitedNext,
            productTrust
          )
        }
      }
    }
  }


  def runWalkRaw(selfId: Int, nodes: Seq[TrustNode], numIterations : Int = 100000): Array[Double] = {

    val nodeMap = nodes.map{n => n.id -> n}.toMap

    val n1 = nodes.head

    val maxPathLength = nodes.size - 1

    def walkFromOrigin() = {
      val totalPathLength = Random.nextInt(maxPathLength - 1) + 1
      walk(n1.id, n1.id, nodeMap, totalPathLength, 0, Set(n1.id), 1D)
    }

    val walkScores = Array.fill(nodes.size)(0D)

    for (_ <- 0 to numIterations) {
      val (id, trust) = walkFromOrigin()
      //  println(s"Returning $id with trust $trust")
      if (id != n1.id) {
        walkScores(id) += trust
      }
    }
    walkScores
  }

  def normalizeScores(scores: Array[Double]): Array[Double] = {
    val sumScore = scores.sum
    scores.map{_ / sumScore}
  }

  def runWalkBatches(
                      selfId: Int,
                      nodes: Seq[TrustNode],
                      batchIterationSize : Int = 10000,
                      epsilon: Double = 1e-6,
                      maxIterations: Int = 100
                    ): Array[Double] = {

    var walkScores = runWalkRaw(selfId, nodes, batchIterationSize)
    var walkProbability = normalizeScores(walkScores)

    var delta = Double.MaxValue
    var iterationNum = 0

    while (delta > epsilon && iterationNum < maxIterations) {

      val batchScores = runWalkRaw(selfId, nodes, batchIterationSize)
      val merged = walkScores.zip(batchScores).map{case (s1, s2) => s1 + s2}
      val renormalized = normalizeScores(merged)
      delta = renormalized.zip(walkProbability).map{case (s1, s2) => Math.pow(Math.abs(s1 - s2), 2)}.sum
      iterationNum += 1
      walkScores = merged
      walkProbability = renormalized
      println(s"Batch number $iterationNum with delta $delta")
    }

    reweightEdges(walkProbability, nodes.map{n => n.id -> n}.toMap)
  }

  def runWalkBatchesFeedback(
                      selfId: Int,
                      nodes: Seq[TrustNode],
                      batchIterationSize : Int = 10000,
                      epsilon: Double = 1e-6,
                      maxIterations: Int = 100
                    ): Seq[TrustNode] = {

    var walkScores = runWalkRaw(selfId, nodes, batchIterationSize)
    var walkProbability = normalizeScores(walkScores)

    var merged = walkScores

    var delta = Double.MaxValue
    var iterationNum = 0


    while (delta > epsilon && iterationNum < maxIterations) {

      val batchScores = runWalkRaw(selfId, nodes, batchIterationSize)
      merged = walkScores.zip(batchScores).map{case (s1, s2) => s1 + s2}
      val renormalized = normalizeScores(merged)
      delta = renormalized.zip(walkProbability).map{case (s1, s2) => Math.pow(Math.abs(s1 - s2), 2)}.sum
      iterationNum += 1
      walkScores = merged
      walkProbability = renormalized
      println(s"Batch number $iterationNum with delta $delta")
    }

    val selfNode = nodes.filter{_.id == selfId}.head
    val others = nodes.filterNot{_.id == selfId}.map{o => o.id -> o}.toMap

    val negativeScores = merged.zipWithIndex.filterNot{_._2 == selfId}.flatMap{ case (score, id) =>
      val other = others(id)
      val negativeEdges = other.negativeEdges
      negativeEdges.filterNot{_.dst == selfId}.map{ ne =>
        ne.dst -> (ne.trust * score / negativeEdges.size)
      }
    }.groupBy(_._1).mapValues(_.map{_._2}.sum)

    negativeScores.foreach{ case (id, negScore) =>
      merged(id) += negScore
    }

    val labelEdges = selfNode.edges.filter(_.isLabel)
    val labelDst = labelEdges.map{_.dst}

    val renormalizedAfterNegative = normalizeScores(merged).zipWithIndex.filterNot{
      case (score, id) => labelDst.contains(id)
    }

    val newEdges = renormalizedAfterNegative.map{ case (score, id) =>
      TrustEdge(selfId, id, score)
    }

    val updatedSelfNode = selfNode.copy(
      edges = labelEdges ++ newEdges
    )
    others.values.toSeq :+ updatedSelfNode
  }


  def reweightEdges(
                     walkProbability: Array[Double],
                     nodeMap: Map[Int, TrustNode]
                   ): Array[Double] = {
    val weightedEdgesAll = Array.fill(nodeMap.size)(0D)

    walkProbability.zipWithIndex.foreach{
      case (prob, id) =>
        // Same issue here as above, need to discard information from untrustworthy original nodes in event
        // walk accidentally trusts them. -- See Jaccard distance comment
        nodeMap(id).edges.foreach{
          e => weightedEdgesAll(e.dst) += e.trust * prob
        }
    }
    weightedEdgesAll
  }


  def updateNode(n: TrustNode, sumTransitives: Map[Int, Double]): TrustNode = {
    n.copy(edges = n.edges.map{ edge =>
      if (!sumTransitives.contains(edge.dst)) edge else {
        edge.copy(trust = sumTransitives(edge.dst))
      }
    } ++ sumTransitives.filterNot(n.edges.map{_.dst}.contains).map{case (id, trust) => TrustEdge(n.id, id, trust)} )
  }

  def updateNodeIn(selfId: Int, nodes: Seq[TrustNode], prevEdges: Array[Double]): Seq[TrustNode] = {
    nodes.filter{_.id == selfId}.map{n =>
      updateNode(n, prevEdges.zipWithIndex.filterNot(_._2 == selfId).map{case (x,y) => y -> x}.toMap)
    } ++ nodes.filterNot(_.id == selfId)
  }

  def runWalk(
               selfId: Int,
               nodes: Seq[TrustNode],
               batchIterationSize : Int = 10000,
               epsilon: Double = 1e-6,
               maxIterations: Int = 100,
               feedbackCycles: Option[Int] = None
             ): Array[Double] = {

    val nodeMap = nodes.map{n => n.id -> n}.toMap

    val weightedEdgeZero  = runWalkBatches(selfId, nodes, batchIterationSize, epsilon, maxIterations)

    val weightedEdgesAll = feedbackCycles.map{ cycles =>

      var prevEdges = weightedEdgeZero
      var nodesCycle = nodes

      (0 until cycles).foreach{ cycle =>
        prevEdges = runWalkBatches(selfId, nodesCycle, batchIterationSize, epsilon, maxIterations)
        nodesCycle = updateNodeIn(selfId, nodesCycle, prevEdges)
      }
      prevEdges

    }.getOrElse(weightedEdgeZero)

    // TODO: Normalize again
    weightedEdgesAll.zipWithIndex.foreach{println}

  //  println(s"n1 id: ${n1.id}")

    weightedEdgesAll

  }
  def runWalkFeedbackUpdateSingleNode(
               selfId: Int,
               nodes: Seq[TrustNode],
               batchIterationSize : Int = 5000,
               epsilon: Double = 1e-5,
               maxIterations: Int = 100,
               feedbackCycles: Int = 3
             ): TrustNode = {

    var nodesCycle = nodes

    (0 until feedbackCycles).foreach{ cycle =>
        println(s"feedback cycle $cycle for node $selfId")
        nodesCycle = runWalkBatchesFeedback(selfId, nodes, batchIterationSize, epsilon, maxIterations)
    }

    nodesCycle.filter(_.id == selfId).head

  }

  def debugRunner(): Unit = {

    val nodes = DataGeneration.generateTestData()

    runWalk(nodes.head.id, nodes)

  }

  def getWalk(selfId: Int, nodes: Seq[TrustNode], numIterations : Int = 100000) = {
    val nodeMap = nodes.map{n => n.id -> n}.toMap
    val n1 = nodes.head
    val maxPathLength = nodes.size - 1

    def walkFromOrigin() = {
      val totalPathLength = Random.nextInt(maxPathLength - 1) + 1
      walk(n1.id, n1.id, nodeMap, totalPathLength, 0, Set(n1.id), 1D)
    }

    val walkScores = Array.fill(nodes.size)(0D)
    for (_ <- 0 to numIterations) {
      val (id, trust) = walkFromOrigin()
      //  println(s"Returning $id with trust $trust")
      if (id != n1.id) {
        walkScores(id) += trust
      }
    }
    val sumScore = walkScores.sum
    val walkProbability = walkScores.map{_ / sumScore}
//    walkProbability.zipWithIndex.foreach{println}
//    n1.positiveEdges.foreach{println}
    val weightedEdgesAll = Array.fill(nodes.size)(0D)
    walkProbability.zipWithIndex.foreach{
      case (prob, id) =>
        // Same issue here as above, need to discard information from untrustworthy original nodes in event
        // walk accidentally trusts them. -- See Jaccard distance comment
        nodeMap(id).edges.foreach{
          e => weightedEdgesAll(e.dst) += e.trust * prob
        }
    }
    weightedEdgesAll.zipWithIndex
  }

  def updateTrustDistro(curNodes: Seq[TrustNode], updateGroups: Map[Int, Seq[TrustEdge]]) = {//todo should dist updates be a map from observer to its edges?
    val curDist: Map[Int, Seq[TrustNode]] = curNodes.groupBy(_.id)
    val updatedNeighborhoods = updateGroups.map { case (id, edgeUpdates: Seq[TrustEdge]) =>
    val curNode: TrustNode = curDist(id).head
      val updateDsts = edgeUpdates.map(_.dst)
      val unchangedEdges = curNode.edges.filterNot {edge => updateDsts.contains(edge.dst)}//should only have one TrustNode per Id
      val newEdges = unchangedEdges ++ edgeUpdates
      curNode.copy(edges = newEdges)
    }
    updatedNeighborhoods
  }

  def getRandomDistro(num: Int = 10) = Seq.tabulate(num){ i =>
      // Change edges to Map[Dst, TrustInfo]
      val edgeIds = Random.shuffle(Seq.tabulate(10) { identity}).take(Random.nextInt(3) + 5)
      TrustNode(i, 0D, 0D, edgeIds.map{ dst =>
        TrustEdge(i, dst, Random.nextDouble())
      })

  }
}
