package org.constellation.testutils

import org.scalatest.enablers.Emptiness
import org.scalatest.matchers.dsl.MatcherFactory1
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.{MatchResult, Matcher}

trait CustomMatchers extends Matchers {

  val containTheSameElements: MatcherFactory1[Seq[_], Emptiness] = (not be empty).and(Matcher { (left: Seq[_]) =>
    MatchResult(
      left.forall(_ == left.head),
      left + " did not contain the same elements",
      left + " did contain the same elements"
    )
  })
}
