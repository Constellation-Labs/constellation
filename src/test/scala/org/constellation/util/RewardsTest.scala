package org.constellation.util
import java.{lang, util}

import atb.common.DefaultRandomGenerator
import atb.interfaces.{Experience, Opinion}
import com.typesafe.scalalogging.Logger
import org.constellation.Fixtures.{id1, id2, id3}
import org.constellation.consensus.RandomData
import org.constellation.trust._

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

  def setupEigenTrust = {
    val eigenTrust = new EigenTrustJ()
    eigenTrust.initialize(0.5D.asInstanceOf[Object], 0.5D.asInstanceOf[Object], 10.asInstanceOf[Object], 0.1.asInstanceOf[Object])
    eigenTrust.setRandomGenerator(new DefaultRandomGenerator(0))
    eigenTrust
  }

  def getRandomOpinions(nodesWithEdges: Seq[TrustNode] = DataGeneration.generateTestData(), time: Int = 0) = {
    val opinionsInput = new util.ArrayList[Opinion]()
    nodesWithEdges.foreach{ node: TrustNode =>
      node.edges.foreach{ edge =>
        val trust = edge.trust / 2 + 0.5 // Revert from -1 to 1 => 0 to 1
        println(trust)
        opinionsInput.add(new Opinion(edge.src, edge.dst, 0, time, trust, Random.nextDouble() / 10 ))
      }
    }
    opinionsInput
  }

  def getSeededEigenTrust(opinionsInput: util.ArrayList[Opinion] = getRandomOpinions()) = {
    val et = setupEigenTrust
    et.processOpinions(opinionsInput)
    et.calculateTrust()
    et
  }

  def getRandomExperiences(trustMap: Map[Integer, lang.Double], time: Int = 0) = {
    val experiences = new util.ArrayList[Experience]()
    trustMap.foreach { case (id, trust) =>
      experiences.add(new Experience(id, 0, time, Random.nextDouble()))//Random double gives us %diff
    }
    experiences
  }

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
    val et = getSeededEigenTrust()
    val trustMap = et.getTrust(0).asScala.toMap
    val experiences = getRandomExperiences(trustMap)
    et.processExperiences(experiences)//Note when outcome is the same scores stay the same
    et.calculateTrust()
    val trustMap2 = et.getTrust(0).asScala.toMap
    assert(trustMap2 != trustMap)
  }

  "Trust updates to Eigentrust" should "update trust ranking" in {
    val et = getSeededEigenTrust()
    val trustMap = et.getTrust(0).asScala.toMap
    val newOpinions = getRandomOpinions(time = 1)
    et.processOpinions(newOpinions)
    et.calculateTrust()
    val trustMap2 = et.getTrust(0).asScala.toMap
    assert(trustMap2 != trustMap)
  }

  "Poor performance" should "reduce trust" in {
    val et = getSeededEigenTrust()
    val trustMap = et.getTrust(0).asScala.toMap
    val experiences = new util.ArrayList[Experience]()
    trustMap.foreach { case (id, trust) =>
      if (id > 20) experiences.add(new Experience(id, 0, 1, 0d))
      else experiences.add(new Experience(id, 0, 1, Random.nextDouble()))
    }
    et.processExperiences(experiences)
    et.calculateTrust()
    val trustMap2 = et.getTrust(0).asScala.toMap
    assert(trustMap2.filterKeys(_ > 20).forall{ case (id, rank) => rank < trustMap(id)})
    }

  "Performance" should "accurately calculate diffs as Experiences" in {
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

  "Trust and random Experience updates" should "accurately update rewards" in {//todo dry
    val et = getSeededEigenTrust()
    val startingDistro: Seq[TrustNode] = SelfAvoidingWalk.getRandomDistro(30)
    val opinionsInput = getRandomOpinions(startingDistro,0)
    et.processOpinions(opinionsInput)
    et.calculateTrust()
    val trustMap = et.getTrust(0).asScala.toMap

    val experienceUpdates = getRandomExperiences(trustMap)
    val trustUpdates = Seq(TrustEdge(1, 2, 0.0), TrustEdge(2, 1, 0.0))
    val updatedTrust = SelfAvoidingWalk.updateTrustDistro(startingDistro, trustUpdates.groupBy(_.src)).toSeq
    val opinionsInput2 = getRandomOpinions(updatedTrust,1)
    et.processOpinions(opinionsInput2)
    et.processExperiences(experienceUpdates)//Note when outcome is the same scores stay the same
    et.calculateTrust()
    val trustMap2 = et.getTrust(0).asScala.toMap
    assert(trustMap != trustMap2)
  }

  "Performance and trust updates" should "be reflected in experience updates" in {
    val et = getSeededEigenTrust()
    val trustMap = et.getTrust(0).asScala.toMap
    val startingDistro = SelfAvoidingWalk.getRandomDistro()
    val trustUpdates = Seq(TrustEdge(1, 2, 0.0), TrustEdge(2, 1, 0.0))
    val updatedTrust = SelfAvoidingWalk.updateTrustDistro(startingDistro, trustUpdates.groupBy(_.src)).toSeq
    val testSnapshotWindow = Seq(
      MetaCheckpointBlock(Map(), None, acceptedCbRound1, dummyCb.checkpoint),
      MetaCheckpointBlock(Map(), None, acceptedCbRound2, dummyCb.checkpoint),
    )
    val opinionsInput2 = getRandomOpinions(updatedTrust,1)
    val perfExperience = performanceExperience(testSnapshotWindow)
    val experiences = new util.ArrayList[Experience]()
    perfExperience.foreach { case (id, trust) =>
      experiences.add(new Experience(id, 0, 1, trust))//Random double gives us %diff
    }
    et.processOpinions(opinionsInput2)
    et.processExperiences(experiences)//Note when outcome is the same scores stay the same
    et.calculateTrust()
    val trustMap2 = et.getTrust(0).asScala.toMap

    assert(trustMap != trustMap2)
  }
}
