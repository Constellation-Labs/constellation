package org.constellation.rewards

import org.mockito.MockitoSugar
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class SoftStakingTest extends AnyFreeSpec with MockitoSugar with Matchers {

  val ignored = Set("ignore")

  "should include SoftStaking address in distribution" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5,
      "ignore" -> 20.5
    )

    val weighted = SoftStaking.weightBySoftStaking(ignored)(10)(distribution)

    weighted.contains(SoftStaking.getAddress) shouldBe true
  }

  "should reduce non-SoftStaking rewards by 40/60 ratio" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5
    )

    val distributionWithIgnored = distribution + ("ignore" -> 30.5)

    val softStakingNodes = 0

    val weighted = SoftStaking.weightBySoftStaking(ignored)(softStakingNodes)(distributionWithIgnored)
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

    val weighted = SoftStaking.weightBySoftStaking(ignored)(softStakingNodes)(distribution)
    val address = SoftStaking.getAddress

    val ratio = 0.4
    val weight = ratio * softStakingNodes + (1 - ratio) * distribution.size

    val expected = (ratio / weight) * distribution.values.sum * softStakingNodes

    weighted(address) shouldBe expected
  }

  "should keep same total reward after weighting" in {
    val total = 999
    val ignoredReward = 100d

    val randomFooReward = Random.nextInt(total).toDouble
    val randomBarReward = total - randomFooReward

    val distribution = Map(
      "foo" -> randomFooReward,
      "boo" -> randomBarReward,
      "ignore" -> ignoredReward
    )

    val weighted = SoftStaking.weightBySoftStaking(ignored)(100)(distribution)

    distribution.values.sum.round shouldBe total + ignoredReward
    weighted.values.sum.round shouldBe total + ignoredReward
  }

  "should keep rewards for ignored addresses" in {
    val distribution = Map(
      "foo" -> 20.5,
      "ignore" -> 30.5
    )

    val weighted = SoftStaking.weightBySoftStaking(ignored)(100)(distribution)

    weighted("ignore") shouldBe 30.5
  }

}
