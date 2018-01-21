package org.constellation.wallet


import java.security.KeyPair

import org.scalatest.FlatSpec

import scala.util.Random
import KeyUtils._

class TestWalletFuncs  extends FlatSpec {

  val kp: KeyPair = makeKeyPair()

  "KeyGen" should "make proper keys" in {
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

    val text = "Yo this some text"
    val inputBytes = text.getBytes()

    val signedOutput = signData(inputBytes)(kp.getPrivate)
    val isLegit = verifySignature(inputBytes, signedOutput)(kp.getPublic)

    assert(isLegit)

  }

  "Key Size" should "verify byte array lengths for encoded keys" in {

    def fill(thunk: => Array[Byte]) =
      Seq.fill(50){thunk}.map{_.length}.distinct

    assert(fill(makeKeyPair().getPrivate.getEncoded) == List(144))
    assert(fill(makeKeyPair().getPublic.getEncoded) == List(88))

  }

  "Address maker" should "create an address" in {
    val addr = publicKeyToAddress(kp.getPublic)
    assert(addr.length > 10)
    assert(addr.toCharArray.distinct.length > 5)
  }

  val sampleTransactionInput = TransactionInputData(kp.getPublic, publicKeyToAddress(kp.getPublic), 1L)

  "Transaction Input Data" should "render to json" in {
    val r = sampleTransactionInput.encode.rendered
    assert(r.contains('['))
    assert(r.length > 50)
    assert(r.toCharArray.distinct.length > 5)
    assert(r.contains('"'))
  }

}
