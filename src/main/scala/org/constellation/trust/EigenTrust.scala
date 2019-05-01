package org.constellation.trust

import java.util

import atb.common.DefaultRandomGenerator
import atb.interfaces.{Experience, Opinion, RandomGenerator}
import atb.trustmodel.{EigenTrust => EigenTrustJ}

import scala.util.Random
import scala.concurrent.JavaConversions._
import scala.collection.JavaConverters._

// See example usage here : https://github.com/djelenc/alpha-testbed/blob/83e669e69463872aa84017051392c885d4183d1d/src/test/java/atb/trustmodel/EigenTrustTMTest.java#L24

object EigenTrust {


//  def main(args: Array[String]): Unit = {


    val nodesWithEdges: List[TrustNode] = DataGeneration.generateTestData()

    val opinionsInput = new util.ArrayList[Opinion]()

    nodesWithEdges.foreach{ node: TrustNode =>
      node.edges.foreach{ edge =>
        val trust = edge.trust / 2 + 0.5 // Revert from -1 to 1 => 0 to 1
        println(trust)
        opinionsInput.add(new Opinion(edge.src, edge.dst, 0, 0, trust, Random.nextDouble() / 10 ))
      }
    }

    val eigenTrust = new EigenTrustJ()
    eigenTrust.initialize(0.5D.asInstanceOf[Object], 0.5D.asInstanceOf[Object], 10.asInstanceOf[Object], 0.1.asInstanceOf[Object])

    eigenTrust.setRandomGenerator(new DefaultRandomGenerator(0))

    eigenTrust.processExperiences(new util.ArrayList[Experience]())
    eigenTrust.processOpinions(opinionsInput)

    eigenTrust.calculateTrust()


    val trustMap = eigenTrust.getTrust(0).asScala.toMap

    trustMap.toSeq.sortBy(_._1).foreach{println}


//  }
}
