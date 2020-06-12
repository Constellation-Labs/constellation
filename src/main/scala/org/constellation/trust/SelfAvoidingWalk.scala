package org.constellation.trust

import com.typesafe.scalalogging.StrictLogging

import scala.util.Random

/**
  * https://en.wikipedia.org/wiki/Node_influence_metric
  * https://en.wikipedia.org/wiki/Self-avoiding_walk
  *
  */
object SelfAvoidingWalk extends StrictLogging {

  final def sample[A](dist: Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item // return so that we don't have to search through the whole distribution
    }
//    println(dist)
    sys.error(f"this should never happen") // needed so it will compile
  }

  // TODO: Make this iterative (simpler, avoid stack depth issue) and memoize visited up to N (small)
  @scala.annotation.tailrec
  def walk(
    selfId: Int,
    currentId: Int,
    nodeMap: Map[Int, TrustNode],
    totalPathLength: Int,
    currentPathLength: Int,
    visited: Set[Int],
    currentTrust: Double
  ): (Int, Double) =
    if (totalPathLength == currentPathLength) {
      currentId -> currentTrust
    } else {

      val n1: TrustNode = nodeMap(currentId)

      // TODO: Visited should have a 'direction' associated to bias the walk not just in terms of trust
      // but also trust derivatives in order to move 'outward' as effectively as possible (to discourage loop formation)
      // otherwise the path length may not matter as the walks will get trapped in the same neighborhood
      // Essentially need topo information from something else processing total edge map

      val visitedNext = visited + currentId

      val normalEdges = n1.normalizedPositiveEdges(visitedNext)
//      logger.debug(s"walk for normalEdges: ${normalEdges.toString()}")

      if (normalEdges.isEmpty) {
        currentId -> currentTrust
      } else {

        val transitionDst = sample(normalEdges)

        // Ignore paths that distrust self (maybe consider ignoring paths that distrust immediate
        // neighbors as well? This is where Jaccard distance is important
        // We need to discard walks where large distance exists from previous
        // (i.e. discard information from distant nodes if they distrust nearby nodes that you trust in general)
        val useInLocalCalculation = nodeMap
          .get(transitionDst)
          .exists(trustNode => trustNode.edges.exists(edge => edge.trust < 0 && edge.dst == selfId))
//        logger.debug(s"walk for useInLocalCalculation: ${useInLocalCalculation.toString()}")

        if (useInLocalCalculation) {
          currentId -> currentTrust
        } else {

          val transitionTrust = normalEdges(transitionDst)
//          logger.debug(s"walk for transitionTrust: ${transitionTrust.toString()}")

          val productTrust = currentTrust * transitionTrust
//          logger.debug(s"walk for productTrust: ${productTrust.toString()}")

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

  def runWalkRaw(selfId: Int, nodes: Seq[TrustNode], numIterations: Int = 100): Array[Double] = {

    val nodeMap = nodes.map { n =>
      n.id -> n
    }.toMap

//    logger.debug(s"runWalkRaw nodeMap: ${nodeMap}")

    val n1 = nodes.head

    val maxPathLength = nodes.size - 1

    def walkFromOrigin() = {
      val totalPathLength = Random.nextInt(maxPathLength) + 1 //note, need min of 3 nodes
      walk(n1.id, n1.id, nodeMap, totalPathLength, 0, Set(n1.id), 1d)
    }
    val numNodes = nodes.maxBy(_.id).id
    val walkScores = Array.fill(numNodes + 1)(0d)
//    logger.debug(s"walkFromOrigin for walkScores ${walkScores.toString()}")

    for (_ <- 0 to numIterations) {
      val (id, trust) = walkFromOrigin()
//      logger.debug(s"Returning $id with trust $trust")
      if (id != n1.id) {
        walkScores(id) += trust
      }
    }

    walkScores
  }

  // Handle edge case where sum is 0
  def normalizeScores(scores: Array[Double]): Array[Double] = {
    val sumScore = scores.sum
    if (sumScore == 0d) scores
    else scores.map { _ / sumScore }
  }

  def normalizeScoresWithIndex(scores: Array[(Double, Int)]): Array[(Double, Int)] = {
    val sumScore = scores.map { _._1 }.sum
    if (sumScore == 0d) scores
    else scores.map { case (k, v) => (k / sumScore) -> v }
  }

  // Need to change to fit to a distribution. Simple way to avoid that for now is splitting pos neg
  def normalizeScoresWithNegative(scores: Array[Double]): Array[Double] = {
    val pos = scores.zipWithIndex.filter(_._1 >= 0)
    val neg = scores.zipWithIndex.filter(_._1 < 0)
    normalizeScoresWithIndex(pos).foreach {
      case (s, i) =>
        scores(i) = s
    }
    normalizeScoresWithIndex(neg).foreach {
      case (s, i) =>
        scores(i) = -1 * s
    }
    scores
  }

  def runWalkBatches(
    selfId: Int,
    nodes: Seq[TrustNode],
    batchIterationSize: Int = 10000,
    epsilon: Double = 1e-6,
    maxIterations: Int = 100
  ): Array[Double] = {

    var walkScores = runWalkRaw(selfId, nodes, batchIterationSize)
    var walkProbability = normalizeScores(walkScores)

    var delta = Double.MaxValue
    var iterationNum = 0

    while (delta > epsilon && iterationNum < maxIterations) {
      val batchScores = runWalkRaw(selfId, nodes, batchIterationSize)
      val merged = walkScores.zip(batchScores).map { case (s1, s2) => s1 + s2 }
      val renormalized = normalizeScores(merged)
      delta = renormalized.zip(walkProbability).map { case (s1, s2) => Math.pow(Math.abs(s1 - s2), 2) }.sum
      iterationNum += 1
      walkScores = merged
      walkProbability = renormalized
      println(s"runWalkBatches - Batch number $iterationNum with delta $delta")
    }

    reweightEdges(walkProbability, nodes.map { n =>
      n.id -> n
    }.toMap)
  }

  def runWalkBatchesFeedback(
    selfId: Int,
    nodes: Seq[TrustNode],
    batchIterationSize: Int = 100,
    epsilon: Double = 1e-6,
    maxIterations: Int = 10
  ): Seq[TrustNode] = {

    var walkScores = runWalkRaw(selfId, nodes, batchIterationSize)
    var walkProbability = normalizeScores(walkScores)

    var merged = walkScores

    var delta = Double.MaxValue
    var iterationNum = 0

    while (delta > epsilon && iterationNum < maxIterations) {
//      logger.debug(s"runWalkBatchesFeedback walkProbability: ${walkScores.toList.toString()}")
//      logger.debug(s"runWalkBatchesFeedback walkScores: ${walkProbability.toList.toString()}")

      val batchScores = runWalkRaw(selfId, nodes, batchIterationSize)

//      logger.debug(s"runWalkBatchesFeedback batchScores: ${batchScores.toList.toString()}")

      merged = walkScores.zip(batchScores).map { case (s1, s2) => s1 + s2 }
//      logger.debug(s"runWalkBatchesFeedback merged: ${merged.toList.toString()}")

      val renormalized = normalizeScores(merged)
//      logger.debug(s"runWalkBatchesFeedback renormalized: ${renormalized.toList.toString()}")

      delta = renormalized.zip(walkProbability).map { case (s1, s2) => Math.pow(Math.abs(s1 - s2), 2) }.sum
//      logger.debug(s"runWalkBatchesFeedback delta: ${delta.toString()}")

      iterationNum += 1
      walkScores = merged
      walkProbability = renormalized
//      logger.debug(s"runWalkBatchesFeedback walkProbability: ${walkProbability.toList.toString()}")
//      println(s"runWalkBatchesFeedback - Batch number $iterationNum with delta $delta")
    }

    val selfNode = nodes.filter { _.id == selfId }.head
    val others = nodes.filterNot { _.id == selfId }.map { o =>
      o.id -> o
    }.toMap

    val negativeScores = merged.zipWithIndex.filterNot { _._2 == selfId }.flatMap {
      case (score, id) =>
        val negativeEdges = others.get(id).map(_.negativeEdges).getOrElse(Seq())
        logger.debug(s"runWalkBatchesFeedback - negativeScores - selfId: $selfId - negativeEdges: $negativeEdges")
        negativeEdges.filterNot { _.dst == selfId }.map { ne =>
          val nanTest = (ne.trust * score / negativeEdges.size)
//          println("nanTest =>" + nanTest)
          ne.dst -> nanTest
        }
    }.groupBy(_._1).mapValues(_.map { _._2 }.sum)

    negativeScores.foreach {
      case (id, negScore) =>
        merged(id) += negScore
    }

    val labelEdges = selfNode.edges.filter(_.isLabel)
    val labelDst = labelEdges.map { _.dst }
//    logger.debug(s"runWalkBatchesFeedback - labelDst ${labelDst.toList.toString()}")

    val doNormalizeScoresWithNegative = normalizeScoresWithNegative(merged)
//    logger.debug(
//      s"runWalkBatchesFeedback - renormalizedAfterNegative ${doNormalizeScoresWithNegative.toList.toString()}"
//    )

    val renormalizedAfterNegative = doNormalizeScoresWithNegative.zipWithIndex.filterNot {
      case (score, id) => labelDst.contains(id)
    }
//    logger.debug(s"runWalkBatchesFeedback - renormalizedAfterNegative ${renormalizedAfterNegative.toList.toString()}")

    val newEdges = renormalizedAfterNegative.map {
      case (score, id) =>
        TrustEdge(selfId, id, score)
    }
//    logger.debug(s"runWalkBatchesFeedback - newEdges ${newEdges.toList.toString()}")

    val updatedSelfNode = selfNode.copy(
      edges = labelEdges ++ newEdges
    )
//    logger.debug(s"runWalkBatchesFeedback - updatedSelfNode ${updatedSelfNode.toString()}")
    val res = others.values.toSeq :+ updatedSelfNode
//    logger.debug(s"runWalkBatchesFeedback - res ${res.toList.toString()}")

    res
  }

  def reweightEdges(
    walkProbability: Array[Double],
    nodeMap: Map[Int, TrustNode]
  ): Array[Double] = {
    val weightedEdgesAll = Array.fill(nodeMap.size)(0d)

    walkProbability.zipWithIndex.foreach {
      case (prob, id) =>
        // Same issue here as above, need to discard information from untrustworthy original nodes in event
        // walk accidentally trusts them. -- See Jaccard distance comment
        nodeMap(id).edges.foreach { e =>
          weightedEdgesAll(e.dst) += e.trust * prob
        }
    }
    weightedEdgesAll
  }

  def updateNode(n: TrustNode, sumTransitives: Map[Int, Double]): TrustNode =
    n.copy(
      edges = n.edges.map { edge =>
        if (!sumTransitives.contains(edge.dst)) edge
        else {
          edge.copy(trust = sumTransitives(edge.dst))
        }
      } ++ sumTransitives.filterNot(n.edges.map { _.dst }.contains).map {
        case (id, trust) => TrustEdge(n.id, id, trust)
      }
    )

  def updateNodeIn(selfId: Int, nodes: Seq[TrustNode], prevEdges: Array[Double]): Seq[TrustNode] =
    nodes.filter { _.id == selfId }.map { n =>
      updateNode(n, prevEdges.zipWithIndex.filterNot(_._2 == selfId).map { case (x, y) => y -> x }.toMap)
    } ++ nodes.filterNot(_.id == selfId)

  def runWalk(
    selfId: Int,
    nodes: Seq[TrustNode],
    batchIterationSize: Int = 10000,
    epsilon: Double = 1e-6,
    maxIterations: Int = 100,
    feedbackCycles: Option[Int] = None
  ): Array[Double] = {

    val nodeMap = nodes.map { n =>
      n.id -> n
    }.toMap

    val weightedEdgeZero = runWalkBatches(selfId, nodes, batchIterationSize, epsilon, maxIterations)

    val weightedEdgesAll = feedbackCycles.map { cycles =>
      var prevEdges = weightedEdgeZero
      var nodesCycle = nodes

      (0 until cycles).foreach { cycle =>
        prevEdges = runWalkBatches(selfId, nodesCycle, batchIterationSize, epsilon, maxIterations)
        nodesCycle = updateNodeIn(selfId, nodesCycle, prevEdges)
      }
      prevEdges

    }.getOrElse(weightedEdgeZero)

    // TODO: Normalize again
    weightedEdgesAll.zipWithIndex.foreach { println }

    //  println(s"n1 id: ${n1.id}")

    weightedEdgesAll

  }

  def runWalkFeedbackUpdateSingleNode(
    selfId: Int,
    nodes: Seq[TrustNode],
    batchIterationSize: Int = 100,
    epsilon: Double = 1e-5,
    maxIterations: Int = 10,
    feedbackCycles: Int = 3
  ): TrustNode = {

    var nodesCycle = nodes
    if (nodesCycle.size > 2) { //note, need min of 3 nodes
      println(s"runWalkFeedbackUpdateSingleNode nodes ${nodes.toList} for node $selfId")
      (0 until feedbackCycles).foreach { cycle =>
        println(s"feedback cycle $cycle for node $selfId")
        nodesCycle = runWalkBatchesFeedback(selfId, nodes, batchIterationSize, epsilon, maxIterations)
      }
    }
    val res: TrustNode = nodesCycle.filter(_.id == selfId).head
    println(s"runWalkFeedbackUpdateSingleNode res: TrustNode ${res} for node $selfId")

    res
  }

  def debugRunner(): Unit = {

    val nodes = DataGeneration.generateTestData()

    runWalk(nodes.head.id, nodes)

  }

  def getWalk(selfId: Int, nodes: Seq[TrustNode], numIterations: Int = 100000) = {
    val nodeMap = nodes.map { n =>
      n.id -> n
    }.toMap
    val n1 = nodes.head
    val maxPathLength = nodes.size - 1

    def walkFromOrigin() = {
      val totalPathLength = Random.nextInt(maxPathLength - 1) + 1
      walk(n1.id, n1.id, nodeMap, totalPathLength, 0, Set(n1.id), 1d)
    }

    val walkScores = Array.fill(nodes.size)(0d)
    for (_ <- 0 to numIterations) {
      val (id, trust) = walkFromOrigin()
      //  println(s"Returning $id with trust $trust")
      if (id != n1.id) {
        walkScores(id) += trust
      }
    }
    val sumScore = walkScores.sum
    val walkProbability = walkScores.map { _ / sumScore }
//    walkProbability.zipWithIndex.foreach{println}
//    n1.positiveEdges.foreach{println}
    val weightedEdgesAll = Array.fill(nodes.size)(0d)
    walkProbability.zipWithIndex.foreach {
      case (prob, id) =>
        // Same issue here as above, need to discard information from untrustworthy original nodes in event
        // walk accidentally trusts them. -- See Jaccard distance comment
        nodeMap(id).edges.foreach { e =>
          weightedEdgesAll(e.dst) += e.trust * prob
        }
    }
    weightedEdgesAll.zipWithIndex
  }

  def updateTrustDistro(curNodes: Seq[TrustNode], updateGroups: Map[Int, Seq[TrustEdge]]) = { //todo should dist updates be a map from observer to its edges?
    val curDist: Map[Int, Seq[TrustNode]] = curNodes.groupBy(_.id)
    val updatedNeighborhoods = updateGroups.map {
      case (id, edgeUpdates: Seq[TrustEdge]) =>
        val curNode: TrustNode = curDist(id).head
        val updateDsts = edgeUpdates.map(_.dst)
        val unchangedEdges = curNode.edges.filterNot { edge =>
          updateDsts.contains(edge.dst)
        } //should only have one TrustNode per Id
        val newEdges = unchangedEdges ++ edgeUpdates
        curNode.copy(edges = newEdges)
    }
    updatedNeighborhoods
  }

  def getRandomDistro(num: Int = 10) = Seq.tabulate(num) { i =>
    // Change edges to Map[Dst, TrustInfo]
    val edgeIds = Random.shuffle(Seq.tabulate(10) { identity }).take(Random.nextInt(3) + 5)
    TrustNode(i, 0d, 0d, edgeIds.map { dst =>
      TrustEdge(i, dst, Random.nextDouble())
    })

  }
}
