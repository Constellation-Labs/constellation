package org.constellation.rewards

import org.constellation.schema.Id

class EigenTrustAgentsTest
    extends FreeSpec
    with BeforeAndAfter
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar {

  "should register agent" in {
    val agents = EigenTrustAgents.empty()
    val id = Id("foo")
    val updated = agents.registerAgent(id)
    updated.contains(id) shouldBe true
  }

  "should unregister agent" in {
    val agents = EigenTrustAgents.empty()
    val id = Id("foo")
    val updated = agents.registerAgent(id)
    updated.contains(id) shouldBe true
    val cleared = updated.unregisterAgent(id)
    cleared.contains(id) shouldBe false
  }

  "Int to Id should be synchronized with Id to Int" in {
    val agents = EigenTrustAgents.empty()
    val id = Id("foo")
    val updated = agents.registerAgent(id)
    val int = updated.get(id).get

    updated.get(id).get shouldBe int
    updated.get(int).get shouldBe id
  }
}
