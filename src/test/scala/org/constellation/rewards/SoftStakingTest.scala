package org.constellation.rewards

import org.mockito.MockitoSugar
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class SoftStakingTest extends AnyFreeSpec with MockitoSugar with Matchers {

  "should include SoftStaking address in distribution" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5
    )

    val weighted = SoftStaking.weightBySoftStaking(10)(distribution)

    weighted.contains(SoftStaking.getAddress) shouldBe true
  }

  "should reduce non-SoftStaking rewards by 40/60 ratio" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5
    )

    val softStakingNodes = 50

    val weighted = SoftStaking.weightBySoftStaking(softStakingNodes)(distribution)
    val address = SoftStaking.getAddress

    val ratio = 0.4
    val weight = ratio * softStakingNodes + (1 - ratio) * distribution.size

    val expected = ((1 - ratio) / weight) * distribution.values.sum

    weighted("foo") shouldBe expected
    weighted("bar") shouldBe expected
  }

  "should assign SoftStaking rewards by 40/60 ratio" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5
    )

    val softStakingNodes = 50

    val weighted = SoftStaking.weightBySoftStaking(softStakingNodes)(distribution)
    val address = SoftStaking.getAddress

    val ratio = 0.4
    val weight = ratio * softStakingNodes + (1 - ratio) * distribution.size

    val expected = (ratio / weight) * distribution.values.sum * softStakingNodes

    weighted(address) shouldBe expected
  }

  "should keep same total reward after weighting" in {
    val total = 999

    val randomFooReward = Random.nextInt(total).toDouble
    val randomBarReward = total - randomFooReward

    val distribution = Map(
      "foo" -> randomFooReward,
      "boo" -> randomBarReward
    )

    val weighted = SoftStaking.weightBySoftStaking(100)(distribution)

    distribution.values.sum.round shouldBe total
    weighted.values.sum.round shouldBe total
  }

}
