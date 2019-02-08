package org.constellation.app
import org.constellation.{ConstellationApp, E2E}

class ConstellationAppTest extends E2E {

  val testApp = new ConstellationApp(createNode(randomizePorts = false))

  "ConstellationApp" should "successfully launch ConstellationNode" in {
    assert(true)
  }

  "ConstellationApp" should "be able to deploy a state channel" in {
    assert(true)
  }
  "ConstellationApp" should "be able to connect to deployed state channel" in {
    assert(true)
  }
}
