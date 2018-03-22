package org.constellation.app

import org.constellation.ConstellationNode
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class SingleAppTest extends  FlatSpec with BeforeAndAfterAll {

  "Single app" should "create a single app node through the regular main method" in {
    ConstellationNode.main(Array("id"))
    // Add verifications here that the app came online properly. Some sort of health check
    // Should be added to the RPC protocol.

  }

  override def afterAll() {
    ConstellationNode.system.terminate()
  }

}
