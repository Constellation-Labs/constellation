package org.constellation.gossip.bisect

import cats.effect.IO
import cats.implicits._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class BisectTest extends AnyFreeSpec with TableDrivenPropertyChecks with Matchers {

  private val testCases = Table[Seq[Int], Int => Boolean, Option[Int]](
    ("sequence", "predicate", "expected result"),
    (Seq(1, 2, 3, 4, 5), _ => false, 1.some),
    (Seq(1, 2, 3, 4, 5), _ < 2, 2.some),
    (Seq(1, 2, 3, 4, 5), _ < 3, 3.some),
    (Seq(1, 2, 3, 4, 5), _ < 4, 4.some),
    (Seq(1, 2, 3, 4, 5), _ < 5, 5.some),
    (Seq(1, 2, 3, 4, 5), _ => true, none[Int]),
    (Seq(1, 2, 3, 4), _ => false, 1.some),
    (Seq(1, 2, 3, 4), _ < 2, 2.some),
    (Seq(1, 2, 3, 4), _ < 3, 3.some),
    (Seq(1, 2, 3, 4), _ < 4, 4.some),
    (Seq(1, 2, 3, 4), _ => true, none[Int]),
    (Seq(), _ => false, none[Int]),
    (Seq(), _ => true, none[Int]),
    (Seq(1), _ => true, none[Int]),
    (Seq(1), _ => false, 1.some)
  )

  forAll(testCases) { (sequence, predicate, expectedResult) =>
    val result = Bisect[IO].runA[Int](predicate.map(IO(_)), sequence).unsafeRunSync()
    result shouldEqual expectedResult
  }

}
