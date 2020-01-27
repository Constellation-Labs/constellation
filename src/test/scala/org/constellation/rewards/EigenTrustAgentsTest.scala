package org.constellation.rewards

import org.constellation.schema.Id
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class EigenTrustAgentsTest
    extends FreeSpec
    with BeforeAndAfter
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar {

  "should register agent" in {
    val agents = EigenTrustAgents.empty()
    val addr = "DAGfoo"
    val updated = agents.registerAgent(addr)
    updated.contains(addr) shouldBe true
  }

  "should unregister agent" in {
    val agents = EigenTrustAgents.empty()
    val addr = "DAGfoo"
    val updated = agents.registerAgent(addr)
    updated.contains(addr) shouldBe true
    val cleared = updated.unregisterAgent(addr)
    cleared.contains(addr) shouldBe false
  }

  "Int to address should be synchronized with address to Int" in {
    val agents = EigenTrustAgents.empty()
    val addr = "DAGfoo"
    val updated = agents.registerAgent(addr)
    val int = updated.get(addr).get

    updated.get(addr).get shouldBe int
    updated.get(int).get shouldBe addr
  }

  "AgentsIterator" - {
    "should return next int" in {
      val iterator = AgentsIterator()
      val next = iterator.next()
      next shouldBe 1
      val nextAfterNext = iterator.next()
      nextAfterNext shouldBe 2
    }
  }
}
