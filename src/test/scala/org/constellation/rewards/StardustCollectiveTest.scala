package org.constellation.rewards

import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class StardustCollectiveTest
    extends AnyFreeSpec
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar
    with MockitoSugar
    with BeforeAndAfter
    with Matchers {

  val ignore = Set("ignore")

  "should include StardustCollective address in distribution" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5,
      "ignore" -> 2.5
    )

    val weighted = StardustCollective.weightByStardust(ignore)(distribution)

    weighted.contains(StardustCollective.getAddress()) shouldBe true
  }

  "should reduce non-StardustCollective rewards by 10%" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5
    )

    val weighted = StardustCollective.weightByStardust(ignore)(distribution)

    println(weighted)
    weighted("foo") shouldBe 111.15
    weighted("bar") shouldBe 289.35
  }

  "should contain Stardust reward as 10% of total non-StardustCollective rewards" in {
    val distribution = Map(
      "foo" -> 123.5,
      "bar" -> 321.5,
      "ignore" -> 100.5
    )

    val weighted = StardustCollective.weightByStardust(ignore)(distribution)
    val address = StardustCollective.getAddress()

    weighted(address) shouldBe distribution("foo") - weighted("foo") + distribution("bar") - weighted("bar")
  }

  "should keep same total reward after weighting" in {
    val total = 999
    val ignored = 100d

    val randomFooReward = Random.nextInt(total).toDouble
    val randomBarReward = total - randomFooReward.toDouble

    val distribution = Map(
      "foo" -> randomFooReward,
      "bar" -> randomBarReward,
      "ignore" -> ignored
    )

    val weighted = StardustCollective.weightByStardust(ignore)(distribution)

    // TODO: Fix precision for Double -> Long/Int conversions!
    distribution.values.sum.round shouldBe total + ignored
    weighted.values.sum.round shouldBe total + ignored
  }

  "should keep rewards for ignored addresses" in {
    val distribution = Map(
      "foo" -> 30.5,
      "ignore" -> 100.5
    )

    val weighted = StardustCollective.weightByStardust(ignore)(distribution)

    weighted("ignore") shouldBe 100.5
  }

}
