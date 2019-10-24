package org.constellation.trust

//import java.io.FileOutputStream

import java.io.{BufferedWriter, FileOutputStream, FileWriter}

import better.files.File

import scala.annotation.tailrec
import scala.util.Random

/**
  * Created by Wyatt on 10/15/19.
  */

case class RunData(nodeSelections: Iterable[(Int, (String, Double))])

object TrustHelpers {
  var (seedNodes, numNodes) = (-1, 100)//ratio of seedNodes/generatorNodes = centrality

  val hashes = generateRandomHashes()

  def randomDoubles = Array.fill[Double](numNodes)(Random.nextDouble())

  def generateRandomHashes(maxProposals: Int = numNodes) = {
    val numDistinct = Random.nextInt(maxProposals) + 1
    (0 until numDistinct).map(_.toString).toArray
  }

  def getRandomHash(hashes: Array[String]) = {
    val max = hashes.length
    val selection = Random.nextInt(max)
    hashes(selection)
  }

  def getRange(numNodes: Int = numNodes) = {
    val tempArray = Array.fill[Double](numNodes)(0D)
    tempArray.zipWithIndex.map{ case (zero, idx) => idx.toDouble}
  }

  def weightProposals(edge: TrustEdge, selfAvoidingWalks: Map[Int, TrustNode], seedNodes: Int) = {
    if (edge.dst < seedNodes) 1.0
    else selfAvoidingWalks(edge.src).edges.filter(_.dst == edge.dst).map(_.trust).headOption.getOrElse(0.0)
  }

  def normalizeHadamard = {
    //todo: hadamard of distro over range of generators, sorted randomly
  }

  def getNodeView(tn: TrustNode, selfAvoidingWalks: Map[Int, TrustNode], nodeProposals: Map[Int, String], roundSeedNodes: Int = seedNodes): (Int, Map[String, Double]) = {
    val dstsToWeights = tn.edges.map(e => (e.dst, weightProposals(e, selfAvoidingWalks, roundSeedNodes)))
    val normalizationFactor = 1.0//todo multiply by normalization: sort dsts by all scores, then multiply by exponential(idx) of idx of dst in range of sorted by scores
    val hashesToWeights = dstsToWeights.map{ case (dst, weight) => (nodeProposals(dst), weight*normalizationFactor)}
    val scoredHashes = hashesToWeights.groupBy { case (dst, weight) => dst}.map{ case (dst, dstWeights) => (dst, dstWeights.map(_._2).sum) }
    (tn.id, scoredHashes)
  }

  def generateRun(roundSeedNodes: Int = seedNodes): Iterable[(Int, (String, Double))] = {

    val nodesWithEdges: Seq[TrustNode] = DataGeneration.generateFullyConnectedTestData(numNodes)
    val trainedEdges = TrustUtil.train(nodesWithEdges)
    val nodeProposals: Map[Int, String] = trainedEdges.map(tn => (tn.id, getRandomHash(hashes))).toMap
    val selfAvoidingWalks: Map[Int, TrustNode] = nodesWithEdges.map(tn => (tn.id, tn)).toMap
    val nodeViews: Iterable[(Int, Map[String, Double])] = selfAvoidingWalks.values.map(getNodeView(_, selfAvoidingWalks, nodeProposals, roundSeedNodes))
    nodeViews.map { case (id, scoredHashes) => (id, scoredHashes.maxBy(_._2)) }
  }

  def saveOutput(output: Iterable[String], ratio: Int, testRun: Int = 0, fileName: String = "raw_output") = {
    val dagDir = System.getProperty("user.home")
    val outputFile = File(dagDir + s"/constellation_test-data/${fileName + "_ratio_" + ratio}_${testRun.toString}").toJava
      if (!outputFile.exists()) {
        outputFile.createNewFile()
      }
      val bw = new BufferedWriter(new FileWriter(outputFile))
      val data: String = output.mkString("\n")
    bw.write(data)
    bw.close()
  }
}
