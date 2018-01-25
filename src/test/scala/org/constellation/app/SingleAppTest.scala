package org.constellation.app

import org.constellation.BlockChainApp
import org.scalatest.FlatSpec

class SingleAppTest extends  FlatSpec {

  "Single app" should "create a single app node through the regular main method" in {
    BlockChainApp.main(Array())
    // Add verifications here that the app came online properly. Some sort of health check
    // Should be added to the RPC protocol.
  }
}
