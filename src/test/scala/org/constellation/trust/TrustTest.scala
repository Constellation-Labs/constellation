package org.constellation.trust
import org.constellation.trust.SelfAvoidingWalk.runWalk
import org.scalatest.FlatSpec
import breeze.stats.distributions._
//import breeze.linalg._
import breeze.plot._
import breeze.linalg._
import breeze.linalg.NumericOps._
import breeze.linalg.operators._

import scala.util.{Random, Try}

class TrustTest extends FlatSpec {

  "Single malicious edge" should "weight good nodes over bad" in {

    val goodNodes: Seq[TrustNode] = Seq.tabulate(10){ i =>

      // Change edges to Map[Dst, TrustInfo]
      val edgeIds: Seq[Int] = Random.shuffle(Seq.tabulate(10) { identity}).take(Random.nextInt(3) + 5)

      TrustNode(i, 0D, 0D, edgeIds.map{ dst =>
        TrustEdge(i, dst, Random.nextDouble())
      })
    }

    val badNodes = Seq.tabulate(10){ i =>
      val iOffset = 10 + i
      val edgeIds = Random.shuffle(Seq.tabulate(10) { _ + 10}).take(Random.nextInt(3) + 5)

      TrustNode(iOffset, 0D, 0D, edgeIds.map{ dst =>
        TrustEdge(i, dst, Random.nextDouble())
      })
    }

    val updatedGoodNodesWithBadEdge: Seq[TrustNode] = goodNodes.tail :+
      goodNodes.head.copy(edges = goodNodes.head.edges :+ TrustEdge(goodNodes.head.id, badNodes.head.id, 0.9D))

    val secondNode = goodNodes.tail.head

    val weightedEdges = runWalk(secondNode.id, updatedGoodNodesWithBadEdge ++ badNodes)

    val good = weightedEdges.slice(0, 10)
    val bad = weightedEdges.slice(10, 20)

    println("Good sum", good.sum)
    println("Bad sum", bad.sum)

    assert(good.sum > bad.sum)


  }

  "Random nodes and edges" should "defend small attack with feedback" in {

    var nodesWithEdges = DataGeneration.generateTestData(15)

    val iterations = 3

    (0 until iterations).foreach{ i =>

      nodesWithEdges = nodesWithEdges.map{
        node =>
           println(s"DebugView ${node.edges}") // Debug view
          SelfAvoidingWalk.runWalkFeedbackUpdateSingleNode(node.id, nodesWithEdges)
      }

    }

    val badNodes = Random.shuffle(nodesWithEdges).take(3)

    val goodNodes = nodesWithEdges.filterNot(badNodes.map{_.id}.contains)

    val testNode = goodNodes.head
    assert(
      testNode.positiveEdges.filter{e => goodNodes.map{_.id}.contains(e.dst)}.map{_.trust}.sum >
      Try{testNode.positiveEdges.filter{e => badNodes.map{_.id}.contains(e.dst)}.map{_.trust}.sum}.getOrElse(0D)
    )
  }

  "Randomly connected nodes with a seed clique" should "defend against a 50% attack" in {
    val clusterSize = 16
    val trustZeroToOne = Random
    var nodesWithEdges: Seq[TrustNode] = DataGeneration.generateFullyConnectedDataWithSeedClique(clusterSize, clusterSize/4, trustZeroToOne)

    val iterations = 3

    (0 until iterations).foreach{ i =>

      nodesWithEdges = nodesWithEdges.map{
        node =>
          println(s"DebugView - iteration: ${i} - idx ${node.id} - ${node.edges} - hasNaNs ${node.edges.filter(_.trust.isNaN)}") // Debug view
          SelfAvoidingWalk.runWalkFeedbackUpdateSingleNode(node.id, nodesWithEdges)
      }

    }
    nodesWithEdges.foreach(n => println("nodesWithEdges: " + n))
    val badNodes = Random.shuffle(nodesWithEdges).take(clusterSize/2)
    val badNodeIds = badNodes.map{_.id}.toSet
    val goodNodes = nodesWithEdges.filterNot(n => badNodeIds.contains(n.id))
    val goodNodeIds = goodNodes.map{_.id}.toSet

    val goodNodePositiveEdgesToPositive = goodNodes.map(_.positiveEdges.filter{e => goodNodes.map{_.id}.contains(e.dst)}.map{_.trust}.sum)




    val goodNodeEdges = goodNodes.map(_.positiveEdges.filter{e => goodNodes.map{_.id}.contains(e.dst)}.map{_.trust}.sum)
    val badNodeEdges = goodNodes.map(_.positiveEdges.filter{e => badNodes.map{_.id}.contains(e.dst)}.map{_.trust}.sum)

    println(goodNodeEdges.sum)
    println(goodNodes.size)
    println(badNodeEdges.sum)
    println(badNodes.size)


    assert(
      goodNodeEdges.sum >
        badNodeEdges.sum
    )
  }

  "Random cliques" should "self organize against a 50% attack" in {
    var nodesWithEdges = DataGeneration.generateTestData(16)

    val iterations = 3

    (0 until iterations).foreach{ i =>

      nodesWithEdges = nodesWithEdges.map{
        node =>
          println(s"DebugView ${node.edges}") // Debug view
          SelfAvoidingWalk.runWalkFeedbackUpdateSingleNode(node.id, nodesWithEdges)
      }

    }

    val badNodes = Random.shuffle(nodesWithEdges).take(8)

    val goodNodes = nodesWithEdges.filterNot(badNodes.map{_.id}.contains)

    val goodNodeEdges = goodNodes.map(_.positiveEdges.filter{e => goodNodes.map{_.id}.contains(e.dst)}.map{_.trust}.sum)
    val badNodeEdges = goodNodes.map(_.positiveEdges.filter{e => badNodes.map{_.id}.contains(e.dst)}.map{_.trust}.sum)

    val zipped = goodNodeEdges.zip(badNodeEdges)
    println(goodNodeEdges.sum)
    println(badNodeEdges.sum)

    println(zipped)
    assert(
      zipped.forall{ case (goodClique, badClique) => goodClique > badClique}
    )
  }

  "A central Clique" should "defend against wider majority in a fully label connected graph" in {
    val nodesWithEdges: Seq[TrustNode] = DataGeneration.generateFullyConnectedTestData(16)


    println(nodesWithEdges.size)
    println(nodesWithEdges.flatMap(_.edges).size)

    val iterations = 3

    val (seedCliqueRandom, widerMajority) = nodesWithEdges.splitAt(4)
    val seedCliqueIds = seedCliqueRandom.map(_.id).toSet
    val allIds = nodesWithEdges.map(_.id).toSet


    var weightedNodesWithEdges: Seq[TrustNode]  = nodesWithEdges.map{ trustNode =>
      val weightedEdges = trustNode.edges.map{ trustEdge =>
      val updatedTrustEdges: TrustEdge = if (seedCliqueIds.contains(trustEdge.dst)) trustEdge.copy(trust = 1D, isLabel = true) else trustEdge
      updatedTrustEdges
    }
      trustNode.copy(edges =  weightedEdges)
      }

    val seedCliqueConnectedEdges: Seq[Seq[TrustEdge]] = weightedNodesWithEdges.filter(n => seedCliqueIds.contains(n.id)).map(t => t.edges.filter(e => seedCliqueIds.contains(e.dst)))
    println("seedCliqueConnectedEdges: " + seedCliqueConnectedEdges)

    /*
    * make sure labeled clique is fully connected and trusted*/
    assert(seedCliqueConnectedEdges.flatten.forall(e => seedCliqueIds.contains(e.dst) && seedCliqueIds.contains(e.src)))
    assert(seedCliqueConnectedEdges.flatten.forall(e => e.trust == 1D))

    /*
    * make sure wider majority is fully connected*/
//    val edgeDsts = nodesWithEdges.map { te => te.edges.map(_.dst).toSet }
//    assert(edgeDsts.forall(_.diff(allIds).isEmpty))//.map(_.edges).forall(edges => edges.map(_.dst).toSet.filterNot(n =>.id == n.id) == allIds))

        (0 until iterations).foreach{ i =>

      weightedNodesWithEdges = nodesWithEdges.map{
        node =>
          println(s"DebugView ${node.edges}") // Debug view
          SelfAvoidingWalk.runWalkFeedbackUpdateSingleNode(node.id, nodesWithEdges)
      }

    }

    val (seedCliqueWeighted, widerMajorityWeighted) = weightedNodesWithEdges.partition(t => seedCliqueIds.contains(t.id))
    println(seedCliqueWeighted)
    assert(seedCliqueWeighted.forall(t => seedCliqueIds.contains(t.id)))
    assert(seedCliqueWeighted.forall(t => t.edges.filter(e => seedCliqueIds.contains(e.dst)).forall(_.trust == 1D)) )


    //
////    val badNodes = Random.shuffle(nodesWithEdges).take(8)
////
////    val goodNodes = nodesWithEdges.filterNot(badNodes.map{_.id}.contains)
//
//    val seedCliquePosEdges = seedCliqueWeighted.map(_.positiveEdges.filter{e => seedCliqueWeighted.map{_.id}.contains(e.dst)}.map{_.trust}.sum)
//    val seedCliqueNegEdges = seedCliqueWeighted.map(_.negativeEdges.filter{e => seedCliqueWeighted.map{_.id}.contains(e.dst)}.map{_.trust}.sum)
//
//    val widerMajorityInterEdges = widerMajorityWeighted.filter(tE => widerMajorityWeighted.map(_.id).contains(tE.id)).map(_.positiveEdges.filter{e => widerMajorityWeighted.map{_.id}.contains(e.dst)}.map{_.trust}.sum)
//    val widerMajorityCliqueEdges = widerMajorityWeighted.map(tE => tE.edges.filter(seedCliqueWeighted.map(_.id).contains(tE)).map(_.positiveEdges.filter{e => seedCliqueWeighted.map{_.id}.contains(e.dst)}.map{_.trust}.sum)
//
//    println(seedCliquePosEdges.sum)
//    println(seedCliqueNegEdges.sum)
//    println(widerMajorityCliqueEdges.sum)
//    println(widerMajorityInterEdges.sum)
//
//    assert(seedCliquePosEdges.sum > seedCliqueNegEdges.sum)
//    assert(widerMajorityCliqueEdges.sum > widerMajorityInterEdges.sum)

  }

  "Convergence test" should "run and plot convergence" in {
    //We need a normalization parameter the score for selecting a common proposal. Should be ratio of seed nodes
    //Define another normalization parameter, centrality: the seed node to generator node ratio
    //We normalize generator nodes so that the
    //normalization to 1 for exp is k /int exp(-kt)
    //normalization to 1 for linear is 1/int = 1/normParam
    //
    //iterate over proposals and weight by reputation/constant for generator/seed nodes. See and what centrality does convergence break down
    val numNodes = 100
//    val maliciousRate = 0.3//use to have them collude on lower scoring hash
    val failureRate = 0.3//how many nodes can fail before diverging
    val (seedNodes, generatorNodes) = (30, 100)//ratio of seedNodes/generatorNodes = centrality

    def generateRandomHashes(maxProposals: Int = numNodes) = {
      val numDistinct = Random.nextInt(maxProposals) + 1
      (0 until numDistinct).map(_.toString).toArray
    }

    val hashes = generateRandomHashes()

    def getRandomHash(hashes: Array[String] = hashes) = {
      val max = hashes.length
      val selection = Random.nextInt(max)
      hashes(selection)
    }



    def powerLaw(operand: Double)(scalingFactor: Double = 3.0) = scala.math.pow(operand, -scalingFactor)
    def exponential(operand: Double)(scalingFactor: Double = 3.0) = scalingFactor * scala.math.exp(-scalingFactor*operand)
    def linear(operand: Double)(scalingFactor: Double = 0.5) = 1-scalingFactor*operand//todo, still need to normalize by dividing by sum after


    def getRange(numNodes: Int = numNodes) = {
      val tempArray = Array.fill[Double](numNodes)(0D)
      tempArray.zipWithIndex.map{ case (zero, idx) => idx.toDouble}
    }

    def weightProposals(edge: TrustEdge, selfAvoidingWalks: Map[Int, TrustNode]) = {
      if (edge.dst < seedNodes) 1.0
      else selfAvoidingWalks(edge.src).edges.filter(_.dst == edge.dst).map(_.trust).headOption.getOrElse(0.0)
    }

    var nodesWithEdges = DataGeneration.generateFullyConnectedTestData(numNodes)

    val iterations = 1
    //first 94, 100
    //second (17,78)
    //(55,6)
    //(31,4)
    //(25,12)
    //third (13,100)
    //fourth (0,100)



    (0 until iterations).foreach{ i =>

      nodesWithEdges = nodesWithEdges.map{
        node =>
          println(s"DebugView ${node.edges}") // Debug view
          SelfAvoidingWalk.runWalkFeedbackUpdateSingleNode(node.id, nodesWithEdges)
      }

    }


//    val nodesWithEdges: Seq[TrustNode] = DataGeneration.generateFullyConnectedTestData(numNodes)
    val nodeProposals = nodesWithEdges.map(tn => (tn.id, getRandomHash())).toMap
    val hashCounts = nodeProposals.values.groupBy(hash => hash).mapValues(_.size)//todo do I need?


    def normalize = {
      //hadamard of distro over range of generators, sorted randomly
    }

    val dummyWalk = Array.fill[Double](numNodes)(Random.nextDouble())
    val selfAvoidingWalks: Map[Int, TrustNode] = nodesWithEdges.map(tn => (tn.id, tn)).toMap
    val nodeViews = selfAvoidingWalks.values.map{ tn =>
      val dstsToWeights = tn.edges.map(e => (e.dst, weightProposals(e, selfAvoidingWalks)))
      val normalize = 1.0//todo multiply by normalization: sort dsts by all scores, then multiply by exponential(idx) of idx of dst in range of sorted by scores
      val hashesToWeights = dstsToWeights.map{ case (dst, weight) => (nodeProposals(dst), weight*normalize) }
      val scoredHashes = hashesToWeights.groupBy { case (dst, weight) => dst}.map{ case (dst, dstWeights) => (dst, dstWeights.map(_._2).sum) }
      (tn.id, scoredHashes)
    }
    val finalViews = nodeViews.map { case (id, scoredHashes) => (id, scoredHashes.maxBy(_._2))}

    finalViews.foreach(println)
    finalViews.map( t => t._2._1).groupBy(k => k).map{ case (s, ss) => (s, ss.size)}.foreach(println)
    //todo: Define converge as >50% of cluster agrees on same hash. Minority then accepts majority
    println("nodeViews: " + nodeViews)
    val dummyWalkVec: DenseVector[Double] = DenseVector(dummyWalk)

//    val dummyPowerLawGradient: DenseVector[Double] = DenseVector(getRange(numNodes).map(powerLaw(_)(3.0)))
    val dummyExpGradient: DenseVector[Double] = DenseVector(getRange(numNodes).map(exponential(_)(3.0)))
//    val dummyLinearGradient: DenseVector[Double] = DenseVector(getRange(numNodes).map(linear(_)(0.5)))

//    val linearRes = dummyLinearGradient * dummyWalkVec
//    val powerLawRes = dummyPowerLawGradient * dummyWalkVec
    val exponentialRes = dummyExpGradient * dummyWalkVec

    val f = Figure()
    val p = f.subplot(0)
    val x = linspace(-5.0, 5.0, 10)

    p += plot(x, exponentialRes)
    p.title = "exponentialRes gradient"

//    println("powerLaw: " + powerLawRes)
    println("exponential: " + exponentialRes)
//    println("linear: " + linearRes)


  }

//  "plot" should "plot" in {
//    val numNodes = 10
//
//    def generateRandomHashes(maxProposals: Int = numNodes) = {
//      val numDistinct = Random.nextInt(maxProposals)
//      (0 until numDistinct + 1).map(_.toString).toArray
//    }
//
//    val hashes = generateRandomHashes()
//
//    def getRandomHash(hashes: Array[String] = hashes) = {
//      val max = hashes.length - 1
//      val selection = Random.nextInt(max)
//      hashes(selection)
//    }
//
//
//    val nodesWithEdges: Seq[TrustNode] = DataGeneration.generateFullyConnectedTestData(numNodes)
//    val nodeProposals = nodesWithEdges.map(tn => (tn.id, getRandomHash())).toMap
//
//
//    //    import breeze.linalg._
////    import breeze.numerics._
////    def fitDistribution(numNodes: Int) = {
////      val x = DenseVector.zeros[Double](numNodes)
////      val test = x(3 to 4) := .5
////      test
////    }
////    import breeze.interpolation._
//
////    import breeze.numerics._
////    import breeze.optimize.minimize
//
//
//    import breeze.signal._
//    //todo: 0 -> 1 normalize?
//    def powerLaw(operand: Double)(scalingFactor: Double = 3.0) = scala.math.pow(operand, -scalingFactor)
//    def exponential(operand: Double)(scalingFactor: Double = 3.0) = scalingFactor * scala.math.exp(-scalingFactor*operand)
//    def linear(operand: Double)(scalingFactor: Double = 0.5) = 1-scalingFactor*operand
//
//
//    def getRange(numNodes: Int = numNodes) = {
//      val tempArray = Array.fill[Double](numNodes)(0D)
//      tempArray.zipWithIndex.map{ case (zero, idx) => idx.toDouble}
//    }
//
//
//    val dummyWalk = Array.fill[Double](numNodes)(Random.nextDouble())
//
//    val dummyWalkVec: DenseVector[Double] = DenseVector(dummyWalk)
//
//    val dummyPowerLawGradient: DenseVector[Double] = DenseVector(getRange(numNodes).map(powerLaw(_)(3.0)))
//    val dummyExpGradient: DenseVector[Double] = DenseVector(getRange(numNodes).map(exponential(_)(3.0)))
//    val dummyLinearGradient: DenseVector[Double] = DenseVector(getRange(numNodes).map(linear(_)(0.5)))
//
//
//    println()
//    println()
//    println()
//    val linearRes = dummyLinearGradient * dummyWalkVec
//    val powerLawRes = dummyPowerLawGradient * dummyWalkVec
//    val exponentialRes = dummyExpGradient * dummyWalkVec
//
//
////    val a = DenseVector(1d, 1d, 1d)//DenseMatrix.zeros[Double](10,1)
////    val b = DenseVector(2d, 2d, 2d)
////    val c = a * b
////    val d = b.:^=(2.0)
////    println(c)
//    println(getRange(5))
////    val expo = new Exponential(0.5)
////    val poi = new Poisson(3.0)
////    println(expo.toString())
//    println("powerLaw: " + powerLawRes)
//    println("exponential: " + exponentialRes)
//    println("linear: " + linearRes)
//
////    val getDist = expo.cdf(1d)
//    val f = Figure()
//    val p = f.subplot(0)
//    val x = linspace(-5.0, 5.0, 10)
//    println(x)
////    val x = linspace(0.0, 1.0)
////    p += plot(x, x.:^=(2.0))
////    p += plot(x, x.:^=(3.0), '.')
//    p.xlabel = "x axis"
//    p.ylabel = "y axis"
////    f.saveas("lines.png")
////    val p2 = f.subplot(1)
////    val p3 = f.subplot(2)
////    val g = breeze.stats.distributions.Gaussian(0,1)
////    p += plot(x, exponentialRes)
////    p.title = "exponentialRes gradient"
////    p += plot(x, powerLawRes)
////    p.title = "powerLawRes gradient"
//    p += plot(x, linearRes)
//    p.title = "linearRes gradient"
//
//
//    //    p2 += hist(g.sample(100000),100)
//    f.saveas("subplots.png")
//    assert(true)
//  }

  }
