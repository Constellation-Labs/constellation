package org.constellation.app

import org.scalatest.{BeforeAndAfterAll, FlatSpec}

// doc
class SingleAppTest extends FlatSpec with BeforeAndAfterAll {

  "Single app" should "create a single app node through the regular main method" in {

    // Need to make sure the heartbeat doesn't mess anything up here.
    //ConstellationNode.main(Array("id"))

    // Add verifications here that the app came online properly. Some sort of health check
    // Should be added to the RPC protocol.

  }

  // doc
  override def afterAll() {
    //  ConstellationNode.system.terminate()
  }

} // end SingleAppTest class
