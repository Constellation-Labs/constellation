//package org.constellation.util
//import java.{lang, util}
//
//import atb.common.DefaultRandomGenerator
//import atb.interfaces.{Experience, Opinion}
//import com.typesafe.scalalogging.Logger
//import org.constellation.Fixtures.{id1, id2, id3}
//import org.constellation.consensus.RandomData
//import org.constellation.trust._
//import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}
//import atb.trustmodel.{EigenTrust => EigenTrustJ}
//import org.constellation.rewards.EigenTrust
//import org.constellation.{DAO, TestHelpers}
//
//import scala.collection.JavaConverters._
//import scala.util.Random
//
///*
//Note: when Experience outcomes are the same, eigentrust scores stay the same.
//When Opinions stay the same, eigentrust scores change
// */
//class RewardsTest extends FlatSpec with BeforeAndAfterAll {
//  import org.constellation.rewards.RewardsManager._
//
//  import RandomData._
//
//  implicit val dao: DAO = TestHelpers.prepareRealDao()
//
//  override def afterAll {
//    dao.unsafeShutdown()
//  }
//
//  val dummyCb = randomBlock(startingTips(go()), keyPairs.head)
//  val acceptedCbRound1 = Seq(randomTransaction, randomTransaction)
//  val acceptedCbRound2 = Seq(randomTransaction, randomTransaction)
//
//  val consensusRound1 = Map(
//    1 -> Set(acceptedCbRound1.head.hash, acceptedCbRound1.tail.head.hash), //todo use id1
//    2 -> Set(acceptedCbRound1.head.hash, acceptedCbRound1.tail.head.hash),
//    3 -> Set(acceptedCbRound1.head.hash)
//  )
//
//  val consensusRound2 = Map(
//    1 -> Set(acceptedCbRound2.head.hash, acceptedCbRound2.tail.head.hash),
//    2 -> Set(acceptedCbRound2.head.hash, acceptedCbRound2.tail.head.hash),
//    3 -> Set(acceptedCbRound2.head.hash)
//  )
//
//  val eigenTrustRes = EigenTrust
//  val totalNeighbors = eigenTrustRes.trustMap.size
//  val logger = Logger("RewardsTest")
//
//  val r = scala.util.Random
//  val epochOneRandom = r.nextInt(epochOne)
//  val epochTwoRandom = epochOne + r.nextInt(epochOne)
//  val epochThreeRandom = epochTwo + r.nextInt(epochOne)
//  val epochFourRandom = epochThree + r.nextInt(epochOne)
//
//  /*
//    Should come from reputation, partition management services
//   */
//  val neighborhoodReputationMatrix = eigenTrustRes.trustMap.map { case (k, v) => (k.toString, v.toDouble) }
//
//  val transitiveReputationMatrix: Map[String, Map[String, Double]] =
//    (0 until totalNeighbors).map(idx => (idx.toString, neighborhoodReputationMatrix)).toMap
//  val partitonChart = (0 until totalNeighbors).map(idx => (idx.toString, Set(idx.toString))).toMap
//
//  val NaNTest = r.nextInt(totalNeighbors).toString
//  neighborhoodReputationMatrix.updated(NaNTest, 0.0) //Ensure perfect behavior doesn't throw Nan
//
//  def setupEigenTrust = {
//    val eigenTrust = new EigenTrustJ()
//    eigenTrust.initialize(
//      0.5d.asInstanceOf[Object],
//      0.5d.asInstanceOf[Object],
//      10.asInstanceOf[Object],
//      0.1.asInstanceOf[Object]
//    )
//    eigenTrust.setRandomGenerator(new DefaultRandomGenerator(0))
//    eigenTrust
//  }
//
//  def getRandomOpinions(nodesWithEdges: Seq[TrustNode] = DataGeneration.generateTestData(), time: Int = 0) = {
//    val opinionsInput = new util.ArrayList[Opinion]()
//    nodesWithEdges.foreach { node: TrustNode =>
//      node.edges.foreach { edge =>
//        val trust = edge.trust / 2 + 0.5 // Revert from -1 to 1 => 0 to 1
////        println(trust)
//        opinionsInput.add(new Opinion(edge.src, edge.dst, 0, time, trust, Random.nextDouble() / 10))
//      }
//    }
//    opinionsInput
//  }
//
//  def getSeededEigenTrust(opinionsInput: util.ArrayList[Opinion] = getRandomOpinions()) = {
//    val et = setupEigenTrust
//    et.processOpinions(opinionsInput)
//    et.calculateTrust()
//    et
//  }
//
//  def getRandomExperiences(trustMap: Map[Integer, lang.Double], time: Int = 0) = {
//    val experiences = new util.ArrayList[Experience]()
//    trustMap.foreach {
//      case (id, trust) =>
//        experiences.add(new Experience(id, 0, time, Random.nextDouble())) //Random double gives us %diff
//    }
//    experiences
//  }
//
//  "rewardForEpoch" should "return correct $DAG ammount" in {
//    assert(rewardDuringEpoch(epochOneRandom) === epochOneRewards)
//    assert(rewardDuringEpoch(epochTwoRandom) === epochTwoRewards)
//    assert(rewardDuringEpoch(epochThreeRandom) === epochThreeRewards)
//    assert(rewardDuringEpoch(epochFourRandom) === epochFourRewards)
//  }
//
//  "total rewards disbursed" should "equal total per epoch within error bar" in {
//    val rewardsDistro = validatorRewards(0, transitiveReputationMatrix, neighborhoodReputationMatrix, partitonChart)
//    val totalDistributionSum = rewardsDistro.values.sum
////    println(totalDistributionSum - epochOneRewards)
//    assert(totalDistributionSum - epochOneRewards <= roundingError)
//  }
//
//  "Experience updates to Eigentrust" should "update the trust matrix" in {
//    val et = getSeededEigenTrust()
//    val trustMap = et.getTrust(0).asScala.toMap
//    val experiences = getRandomExperiences(trustMap)
//    et.processExperiences(experiences) //Note when outcome is the same scores stay the same
//    et.calculateTrust()
//    val trustMap2 = et.getTrust(0).asScala.toMap
//    assert(trustMap2 != trustMap)
//  }
//
//  "Trust updates to Eigentrust" should "update trust ranking" in {
//    val et = getSeededEigenTrust()
//    val trustMap = et.getTrust(0).asScala.toMap
//    val newOpinions = getRandomOpinions(time = 1)
//    et.processOpinions(newOpinions)
//    et.calculateTrust()
//    val trustMap2 = et.getTrust(0).asScala.toMap
//    assert(trustMap2 != trustMap)
//  }
//
//  ("Poor performance" should "reduce trust").ignore {
//    val et = getSeededEigenTrust()
//    val trustMap = et.getTrust(0).asScala.toMap
//    val experiences = new util.ArrayList[Experience]()
//    trustMap.foreach {
//      case (id, trust) =>
//        if (id > 20) experiences.add(new Experience(id, 0, 1, 0d))
//        else experiences.add(new Experience(id, 0, 1, Random.nextDouble()))
//    }
//    et.processExperiences(experiences)
//    et.calculateTrust()
//    val trustMap2 = et.getTrust(0).asScala.toMap
//    assert(trustMap2.filterKeys(_ > 20).forall {
//      case (id, rank) =>
//        val diff = trustMap(id) - rank
////      println(s"diff: $diff >= trustRoundingError: ${diff >= trustRoundingError} - rank: $rank - trustMap: ${trustMap(id)}")
//        diff >= EigenTrust.trustRoundingError
//    })
//  }
//
//  "Performance" should "accurately calculate diffs as Experiences" in {
//    val testSnapshotWindow = Seq(
//      MetaCheckpointBlock(consensusRound1, None, acceptedCbRound1, dummyCb.checkpoint),
//      MetaCheckpointBlock(consensusRound2, None, acceptedCbRound2, dummyCb.checkpoint)
//    )
//    val correctResult = Map(1 -> 0.0, 2 -> 0.0, 3 -> 0.5)
//    val res = performanceExperience(testSnapshotWindow)
//    assert(res == correctResult)
//  }
//}
