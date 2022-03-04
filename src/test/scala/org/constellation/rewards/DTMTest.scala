package org.constellation.rewards

import org.mockito.MockitoSugar
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class DTMTest extends AnyFreeSpec with MockitoSugar with Matchers {

  val avgSnapshotsPerMonth = 43110
  val monthlyReward = 50000000000000d

  val perSnapshot = monthlyReward / avgSnapshotsPerMonth

  "should include DTM address in distribution" in {
    val distribution = Map("foo" -> 123.5)

    val weighted = DTM.weightByDTM(distribution)

    weighted.contains(DTM.getAddress) shouldBe true
  }

  "should assign DTM reward based on avg snapshot per month" in {
    val distribution = Map("foo" -> 123.5, "bar" -> 123.5)

    val weighted = DTM.weightByDTM(distribution)

    weighted(DTM.getAddress) shouldBe perSnapshot
  }

  "should reduce existing rewards by DTM reward" in {
    val distribution = Map("foo" -> 10000000000000d, "bar" -> 100000000000000d)
    val weighted = DTM.weightByDTM(distribution)

    val toDistribute = distribution.values.sum - perSnapshot

    weighted("foo") shouldBe toDistribute / 2
    weighted("bar") shouldBe toDistribute / 2
  }

  "should reduce rewards to 0 if not enough rewards in the pool" in {
    val distribution = Map("foo" -> 123.5, "bar" -> 123.5)

    val weighted = DTM.weightByDTM(distribution)

    weighted(DTM.getAddress) shouldBe perSnapshot
    weighted("foo") shouldBe 0d
    weighted("bar") shouldBe 0d
  }

}
