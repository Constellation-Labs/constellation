package org.constellation.checkpoint

import org.scalatest.{FreeSpec, Matchers}

class TopologicalSortTest extends FreeSpec with Matchers {
  val nonSorted = Seq[(String, String)](("a", "b"), ("b", "d"), ("c", "b"), ("c", "d"))

  "sortTopologically sorts based on edges" in {
    val sorted = TopologicalSort.sortTopologically(nonSorted)
    sorted shouldBe List("a", "c", "b", "d")
  }
}
