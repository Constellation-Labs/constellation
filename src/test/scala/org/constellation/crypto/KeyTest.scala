package org.constellation.crypto

import org.scalatest.FlatSpec

/** Documentation. */
class KeyTest extends FlatSpec {

  private val testKeys = Seq.fill(20){KeyUtils.makeKeyPair()}

  "Hex encodings public" should "strip prefix and decode public keys properly" in {

    testKeys.foreach { kp =>
      val hex = KeyUtils.publicKeyToHex(kp.getPublic)
      assert(KeyUtils.hexToPublicKey(hex) == kp.getPublic)
    }
  }

  "Hex encodings private" should "strip prefix and decode private keys properly" in {

    testKeys.foreach{ kp =>
      val hex = KeyUtils.privateKeyToHex(kp.getPrivate)
      assert(KeyUtils.hexToPrivateKey(hex) == kp.getPrivate)
    }

  }

}

