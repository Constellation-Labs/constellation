package org.constellation.trust

import scala.annotation.tailrec
import scala.util.Random

/**
  * Created by Wyatt on 10/15/19.
  */
object TrustUtil {

  def powerLaw(operand: Double)(scalingFactor: Double = 3.0) = scala.math.pow(operand, -scalingFactor)

  def exponential(operand: Double)(scalingFactor: Double = 3.0) = scalingFactor * scala.math.exp(-scalingFactor*operand)

  def linear(operand: Double)(scalingFactor: Double = 0.5) = 1-scalingFactor*operand//todo, still need to normalize by dividing by sum after

  @tailrec
  def train(nodesWithEdges: Seq[TrustNode], iterations: Int = 1): Seq[TrustNode] = {
    val trainedEdges = nodesWithEdges.map{ node =>
      //      println(s"DebugView ${node.edges}") // Debug view
      SelfAvoidingWalk.runWalkFeedbackUpdateSingleNode(node.id, nodesWithEdges)
    }
    if (iterations <= 1) trainedEdges
    else train(trainedEdges, iterations - 1)
  }


}
