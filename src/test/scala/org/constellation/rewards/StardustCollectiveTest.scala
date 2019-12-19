package org.constellation.rewards

import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.util.Random

class StardustCollectiveTest
    extends FreeSpec
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar
    with MockitoSugar
    with BeforeAndAfter
    with Matchers {

  "should include StardustCollective address in distribution" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5
    )

    val weighted = StardustCollective.weightByStardust(distribution)

    weighted.contains(StardustCollective.getAddress()) shouldBe true
  }

  "should reduce non-StardustCollective rewards by 10%" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5
    )

    val weighted = StardustCollective.weightByStardust(distribution)

    println(weighted)
    weighted("foo") shouldBe 111.15
    weighted("bar") shouldBe 289.35
  }

  "should contain Stardust reward as 10% of total non-StardustCollective rewards" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5
    )

    val weighted = StardustCollective.weightByStardust(distribution)
    val address = StardustCollective.getAddress()

    weighted(address) shouldBe distribution("foo") - weighted("foo") + distribution("bar") - weighted("bar")
  }

  "should keep same total reward after weighting" in {
    val total = 999

    val randomFooReward = Random.nextInt(total).toDouble
    val randomBarReward = total - randomFooReward.toDouble

    val distribution = Map(
      "foo" -> randomFooReward,
      "bar" -> randomBarReward
    )

    val weighted = StardustCollective.weightByStardust(distribution)

    // TODO: Fix precision for Double -> Long/Int conversions!
    distribution.values.sum.round shouldBe total
    weighted.values.sum.round shouldBe total
  }

}
