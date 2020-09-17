package org.constellation.checkpoint

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class TopologicalSortTest extends AnyFreeSpec with Matchers {
  val nonSorted = Seq[(String, String)](("a", "b"), ("b", "d"), ("c", "b"), ("c", "d"))

  "sortTopologically sorts based on edges" in {
    val sorted = TopologicalSort.sortTopologically(nonSorted)
    sorted shouldBe List("a", "c", "b", "d")
  }
}
