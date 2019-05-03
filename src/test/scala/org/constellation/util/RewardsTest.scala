package org.constellation.util
import java.util

import atb.common.DefaultRandomGenerator
import atb.interfaces.{Experience, Opinion}
import com.typesafe.scalalogging.Logger
import org.constellation.Fixtures.{id1, id2, id3}
import org.constellation.consensus.RandomData
import org.constellation.consensus.RandomData.{keyPairs, randomBlock, startingTips}
import org.constellation.trust._
import org.constellation.trust.EigenTrust.{eigenTrust, nodesWithEdges, opinionsInput, trustMap}
import org.scalatest.FlatSpec
import atb.trustmodel.{EigenTrust => EigenTrustJ}

import scala.collection.JavaConverters._
import scala.util.Random

/*
Note: when Experience outcomes are the same, eigentrust scores stay the same.
When Opinions stay the same, eigentrust scores change
 */
class RewardsTest extends FlatSpec {
  import org.constellation.util.Rewards._

  import RandomData._

  val dummyCb = randomBlock(startingTips, keyPairs.head)
  val acceptedCbRound1 = Seq(randomTransaction, randomTransaction)
  val acceptedCbRound2 = Seq(randomTransaction, randomTransaction)
  val consensusRound1 = Map(
    1 -> Set(acceptedCbRound1.head.hash, acceptedCbRound1.tail.head.hash),//todo use id1
    2 -> Set(acceptedCbRound1.head.hash, acceptedCbRound1.tail.head.hash),
    3 -> Set(acceptedCbRound1.head.hash)
  )
  val consensusRound2 = Map(
    1 -> Set(acceptedCbRound2.head.hash, acceptedCbRound2.tail.head.hash),
    2 -> Set(acceptedCbRound2.head.hash, acceptedCbRound2.tail.head.hash),
    3 -> Set(acceptedCbRound2.head.hash)
  )

  val eigenTrustRes = EigenTrust
  val totalNeighbors = eigenTrustRes.trustMap.size
  val logger = Logger("RewardsTest")

  val r = scala.util.Random
  val epochOneRandom = r.nextInt(epochOne)
  val epochTwoRandom = epochOne + r.nextInt(epochOne)
  val epochThreeRandom = epochTwo + r.nextInt(epochOne)
  val epochFourRandom = epochThree + r.nextInt(epochOne)

  /*
    Should come from reputation, partition management services
   */
  val neighborhoodReputationMatrix = eigenTrustRes.trustMap.map { case (k, v) => (k.toString, v.toDouble) }
  val transitiveReputationMatrix: Map[String, Map[String, Double]] =
    (0 until totalNeighbors).map(idx => (idx.toString, neighborhoodReputationMatrix)).toMap
  val partitonChart = (0 until totalNeighbors).map(idx => (idx.toString, Set(idx.toString))).toMap

  val NaNTest = r.nextInt(totalNeighbors).toString
  neighborhoodReputationMatrix.updated(NaNTest, 0.0)//Ensure perfect behavior doesn't throw Nan

  "rewardForEpoch" should "return correct $DAG ammount" in {
    assert(rewardForEpoch(epochOneRandom) === epochOneRewards)
    assert(rewardForEpoch(epochTwoRandom) === epochTwoRewards)
    assert(rewardForEpoch(epochThreeRandom) === epochThreeRewards)
    assert(rewardForEpoch(epochFourRandom) === epochFourRewards)
  }

  "total rewards disbursed" should "equal total per epoch within error bar" in {
    val rewardsDistro = validatorRewards(0,
      transitiveReputationMatrix,
      neighborhoodReputationMatrix,
      partitonChart)
    val totalDistributionSum = rewardsDistro.values.sum
    println(totalDistributionSum - epochOneRewards)
    assert(totalDistributionSum - epochOneRewards <= roundingError)
  }

  "Experience updates to Eigentrust" should "update the trust matrix" in {
    val experiences = new util.ArrayList[Experience]()
    trustMap.foreach { case (id, trust) =>
      experiences.add(new Experience(id, 0, 0, Random.nextDouble()))//Random double gives us %diff
    }
    eigenTrust.processExperiences(experiences)//Note when outcome is the same scores stay the same
    eigenTrust.calculateTrust()
    val trustMap2 = eigenTrust.getTrust(0).asScala.toMap
    assert(trustMap2 != trustMap)
  }

  "Trust updates to Eigentrust" should "update trust ranking" in {
    val nodesWithEdges2 = DataGeneration.generateTestData()
    val opinionsInput2 = new util.ArrayList[Opinion]()
    nodesWithEdges2.foreach { node =>
      node.edges.foreach { edge =>
        val trust = edge.trust / 2 + 0.5 // Revert from -1 to 1 => 0 to 1
        opinionsInput2.add(new Opinion(edge.src, edge.dst, 0, 1, trust, Random.nextDouble() / 10))
      }
    }
    eigenTrust.processOpinions(opinionsInput2)
    eigenTrust.calculateTrust()
    val trustMap2 = eigenTrust.getTrust(0).asScala.toMap
    assert(trustMap2 != trustMap)
  }

  "Poor performance" should "reduce trust" in {
    val et = new EigenTrustJ()
    val experiences = new util.ArrayList[Experience]()
    trustMap.foreach { case (id, trust) =>
      if (id > 20) experiences.add(new Experience(id, 0, 1, 0d))
      else experiences.add(new Experience(id, 0, 1, Random.nextDouble()))
    }

    et.initialize(0.5D.asInstanceOf[Object], 0.5D.asInstanceOf[Object], 10.asInstanceOf[Object], 0.1.asInstanceOf[Object])

    et.setRandomGenerator(new DefaultRandomGenerator(0))
    et.processExperiences(experiences)
    et.calculateTrust()
    val trustMap2 = et.getTrust(0).asScala.toMap
    println
    assert(trustMap2.filterKeys(_ > 20).forall{ case (id, rank) => rank < trustMap(id)})
    }

  "performanceExperience" should "accurately calculate diffs" in {
    val testSnapshotWindow = Seq(
      MetaCheckpointBlock(consensusRound1, None, acceptedCbRound1, dummyCb.checkpoint),
      MetaCheckpointBlock(consensusRound2, None, acceptedCbRound2, dummyCb.checkpoint),
    )
    val correctResult = Map(1 -> 0.0, 2 -> 0.0, 3 -> 0.5)
    val res = performanceExperience(testSnapshotWindow)
    assert(res == correctResult)
  }

  "Trust updates" should "accurately update edges" in {
    val startingDistro = SelfAvoidingWalk.getRandomDistro()
    val trustUpdates = Seq(TrustEdge(1, 2, 0.0), TrustEdge(2, 1, 0.0))
    val updatedTrust = SelfAvoidingWalk.updateTrustDistro(startingDistro, trustUpdates.groupBy(_.src))
    println(startingDistro)
    println(updatedTrust)

    assert(startingDistro != updatedTrust)
  }

  "Trust and updates" should "accurately update rewards" in {//todo dry
    val startingDistro: Seq[TrustNode] = SelfAvoidingWalk.getRandomDistro(30)
    val trustUpdates = Seq(TrustEdge(1, 2, 0.0), TrustEdge(2, 1, 0.0))
    val distro2 = SelfAvoidingWalk.updateTrustDistro(startingDistro, trustUpdates.groupBy(_.src))
//    val nodesWithEdges2: Seq[TrustNode] = DataGeneration.generateTestData()
    val opinionsInput1 = new util.ArrayList[Opinion]()
    startingDistro.foreach { node =>
      node.edges.foreach { edge =>
        val trust = edge.trust / 2 + 0.5 // Revert from -1 to 1 => 0 to 1
        opinionsInput1.add(new Opinion(edge.src, edge.dst, 0, 1, trust, Random.nextDouble() / 10))
      }
    }
    eigenTrust.processOpinions(opinionsInput1)
    eigenTrust.calculateTrust()
    val trustMap1 = eigenTrust.getTrust(0).asScala.toMap

    val opinionsInput2 = new util.ArrayList[Opinion]()
    distro2.foreach { node =>
      node.edges.foreach { edge =>
        val trust = edge.trust / 2 + 0.5 // Revert from -1 to 1 => 0 to 1
        opinionsInput1.add(new Opinion(edge.src, edge.dst, 0, 2, trust, Random.nextDouble() / 10))
      }
    }
    eigenTrust.processOpinions(opinionsInput2)
    eigenTrust.calculateTrust()
    val trustMap2 = eigenTrust.getTrust(0).asScala.toMap

    assert(startingDistro != distro2)
  }

  "Trust and Experience updates" should "accurately update rewards" in {
    val startingDistro = SelfAvoidingWalk.getRandomDistro()
    val trustUpdates = Seq(TrustEdge(1, 2, 0.0), TrustEdge(2, 1, 0.0))
    val updatedTrust = SelfAvoidingWalk.updateTrustDistro(startingDistro, trustUpdates.groupBy(_.src))
    val testSnapshotWindow = Seq(
      MetaCheckpointBlock(Map(), None, acceptedCbRound1, dummyCb.checkpoint),
      MetaCheckpointBlock(Map(), None, acceptedCbRound2, dummyCb.checkpoint),
    )
  }
}
