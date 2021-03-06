package org.constellation.invertedmap

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class InvertedMapTest extends AnyFreeSpec with Matchers {
  type Node = String

  sealed trait NodeState
  object Ready extends NodeState
  object Offline extends NodeState

  type SingleObservation = Map[Node, NodeState]

  val allReady: SingleObservation = Map(
    "node1" -> Ready,
    "node2" -> Ready,
    "node3" -> Ready
  )

  val oneOffline: SingleObservation = Map(
    "node1" -> Ready,
    "node2" -> Offline,
    "node3" -> Ready
  )

  val twoOffline: SingleObservation = Map(
    "node1" -> Offline,
    "node2" -> Offline,
    "node3" -> Ready
  )

  val m = InvertedMap(("node1", allReady), ("node2", allReady), ("node3", oneOffline))

  "creates empty map" in {
    InvertedMap.empty[Node, SingleObservation] should be(empty)
  }

  "has a correct size" in {
    m should have size 2
  }

  "allows to get a value" in {
    m.get("node2").isDefined shouldBe true
    m.get("unknown").isDefined shouldBe false
  }

  "allows to remove a value" in {
    (m - "node1" - "node2").get("node2") shouldBe None
  }

  "allows to add a new value" in {
    (m + (("node4", allReady))).get("node4").isDefined shouldBe true
  }

  "does not allow to set same key again" in {
    assertThrows[AssertionError] {
      m + (("node3", allReady))
    }
  }

  "updated" - {
    "does not change the size of the map if value already exists" in {
      val previousSize = m.size

      m.updated("node4", allReady) should have size previousSize
    }

    "changes the size of the map if value does not exists" in {
      val previousSize = m.size

      m.updated("node4", twoOffline) should have size (previousSize + 1)
    }

    "updates a key properly" in {
      val newMap = m.updated("node2", twoOffline)

      newMap.keysSize(allReady) shouldBe 1
      newMap.keysSize(twoOffline) shouldBe 1
    }
  }

  "isConverged" - {
    "returns true if map has single value" in {
      (m - "node3" isConverged) shouldBe true
    }

    "returns false if map has more than a single value" in {
      (m isConverged) shouldBe false
    }
  }

  "keySize" - {
    "returns size of the keys assigned to the given value" in {
      m.keysSize(allReady) shouldBe 2
      m.keysSize(oneOffline) shouldBe 1
      m.keysSize(twoOffline) shouldBe 0
    }
  }

  "filter" - {
    "returns correct size" in {
      m.filter(_._2 == oneOffline) should have size 1
    }
  }
}
