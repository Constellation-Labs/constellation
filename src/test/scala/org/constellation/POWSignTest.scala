package org.constellation

import org.constellation.util.ProductHash
import org.scalatest.FlatSpec

case class TestSignable(a: String, b: Int) extends ProductHash

import constellation._

class POWSignTest extends FlatSpec {


  "POW" should "do a proof of work and verify it" in {

    val input = "Input"
    val diff = Some(1)
    val nonce = proofOfWork(input, diff)
    val hash = hashNonce(input, nonce)
    assert(hash.startsWith("0"))
    assert(hash.length > 10)
    assert(nonce.nonEmpty)
    assert(verifyPOW(input, nonce, diff))

  }

  "Simple POWSign" should "sign and pow a simple fake case class properly" in {

    val kp = makeKeyPair()
    val data = TestSignable("a", 1)
    val powSigned = powSignData(data, Seq(kp), 1)
    assert(powSigned.data == data)
    println(powSigned.nonce)
    assert(powSigned.validSignatures)
    assert(powSigned.validPOW)
    assert(powSigned.valid)

  }


}
