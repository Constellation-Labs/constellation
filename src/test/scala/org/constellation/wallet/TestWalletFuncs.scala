package org.constellation.wallet

import java.security.spec.ECGenParameterSpec

import org.scalatest.FlatSpec
import java.security.{PrivateKey, SecureRandom, Signature}


class TestWalletFuncs  extends FlatSpec {

  "KeyGen" should "make proper keys" in {
    val kp = KeyUtils.makeKeyPair()
    val privK = kp.getPrivate.toString
    val pubK = kp.getPublic.toString
    Seq(privK, pubK).foreach { pk =>
      assert(pk.length > 50)
      assert(pk.contains(":"))
      assert(pk.contains("X:"))
      assert(pk.split("\n").length > 2)
    }
  }

  "Signature" should "sign and verify output" in {

    val kp = KeyUtils.makeKeyPair()
    val text = "Yo this some text"
    val inputBytes = text.getBytes()

    val signedOutput = KeyUtils.signData(inputBytes)(kp.getPrivate)
    val isLegit = KeyUtils.verifySignature(inputBytes, signedOutput)(kp.getPublic)

    assert(isLegit)

  }


}
