package org.constellation.trust
import better.files.File
import org.constellation.trust.SelfAvoidingWalk.runWalk
import org.scalatest.FlatSpec
//import breeze.stats.distributions._

import scala.annotation.tailrec
import scala.collection.immutable
import scala.io.Source
//import breeze.linalg._
//import breeze.plot._
//import breeze.linalg._
//import breeze.linalg.NumericOps._
//import breeze.linalg.operators._

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
    val nodesWithEdges: Seq[TrustNode] = DataGeneration.generateFullyConnectedTestData(8)


    println(nodesWithEdges.size)
    println(nodesWithEdges.flatMap(_.edges).size)

    val iterations = 3

    val (seedCliqueRandom, widerMajority) = nodesWithEdges.splitAt(1)
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

  def generateAndSaveConvergenceData(seedRatios: Int) = {
    val testObj = TrustHelpers
    val numTestRounds = 3//todo make global somewhere
    val finalViews: Seq[Iterable[(Int, (String, Double))]] = (0 until numTestRounds).toList.map(i => testObj.generateRun(seedRatios))
    val proposalDist = finalViews.map((fv: Iterable[(Int, (String, Double))]) => fv.map(t => t._2._1).groupBy(k => k).map{ case (s, ss) => (s, ss.size)})
    proposalDist.foreach(println)
    val snapshotVoteRounds = finalViews.map { iter =>
      val res = iter
        .map( t => t._2._1)
        .groupBy(k => k)
        .map{ case (s, ss) => s + "|" + ss.size.toString}
      res
    }
    snapshotVoteRounds.zipWithIndex.foreach { case (votes, voteNum) =>
      TrustHelpers.saveOutput(votes, seedRatios, voteNum)
      //    println("hashes: " + testObj.hashes.distinct.length.toString)
    }
  }
  def loadTestRunFiles(testRunNum: Int, ratio: Int, fileName: String = "raw_output") =
    Source.fromFile(System.getProperty("user.home") + s"/constellation_test-data/${fileName + "_ratio_" + ratio}_${testRunNum.toString}")

  def getAvgDistinctProposals(totalRuns: Int, ratio: Int): Double = {
    val proposalsPerRun = (0 until totalRuns).map { runNum =>
      val sumDistinctProposals = loadTestRunFiles(runNum, ratio).getLines().map { row =>
        val Array(snapshotHash, numVotes) = row.split('|').map(_.trim)
        snapshotHash
      }.length
      sumDistinctProposals
    }
    proposalsPerRun.sum/totalRuns.toDouble
  }


  def getAvgConvergencePerSeedRatio(totalRuns: Int, ratioUpperBound: Int) : Array[Double] = {
    val range = (0 until ratioUpperBound by 10)//todo note that this goes to n-1 by 10
      .map(getAvgDistinctProposals(totalRuns, _))
    range.toArray
  }

  "Convergence test" should "run and plot convergence" in {
    val seedRatios = 40
    (0 until seedRatios by 10)//todo note that this goes to n-1 by 10
      .foreach(ratio => generateAndSaveConvergenceData(ratio))

    assert(true)
  }

//  "plot" should "plot" in {
//    val ratioUpperBound = 40
//    val numTestRounds = 3
//    val domain = DenseVector(getAvgConvergencePerSeedRatio(numTestRounds, ratioUpperBound))
//    println(domain)
//    //    val dv = DenseVector(10.5, 9.1, 4.4, 2.6)//todo fill by step in seedRatios
//    val f = Figure()
//    val p = f.subplot(0)
//    val x = linspace(0.0, 30.0, domain.length)//length = dv.length
//    p += plot(x, domain)
//    p.xlabel = "Seed nodes (out of 100)"
//    p.ylabel = "Unique proposal count avg"
//    f.saveas("convergence_over_seed_ratios.png")
//    assert(true)
//  }

}
