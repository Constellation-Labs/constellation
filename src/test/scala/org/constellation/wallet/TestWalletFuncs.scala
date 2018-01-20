package org.constellation.wallet

import org.scalatest.FlatSpec

class TestWalletFuncs  extends FlatSpec {

  "KeyGen" should "make proper keys" in {
    val kp = KeyGen.makeKeyPair()
    val privK = kp.getPrivate.toString
    val pubK = kp.getPublic.toString
    Seq(privK, pubK).foreach { pk =>
      assert(pk.length > 50)
      assert(pk.contains(":"))
      assert(pk.contains("X:"))
      assert(pk.split("\n").length > 2)
    }
  }

}
