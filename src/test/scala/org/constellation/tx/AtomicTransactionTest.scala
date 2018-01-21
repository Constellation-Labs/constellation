package org.constellation.tx

import java.security.KeyPair

import org.constellation.tx.AtomicTransaction._
import org.constellation.wallet.KeyUtils._
import org.scalatest.FlatSpec

class AtomicTransactionTest extends FlatSpec {

  val kp: KeyPair = makeKeyPair()

  val sampleTransactionInput = TransactionInputData(kp.getPublic, publicKeyToAddress(kp.getPublic), 1L)

  "Transaction Input Data" should "render to json" in {
    val r = sampleTransactionInput.encode.rendered
    assert(r.contains('['))
    assert(r.length > 50)
    assert(r.toCharArray.distinct.length > 5)
    assert(r.contains('"'))
  }

  "Transaction Encoding" should "encode and decode transactions" in {
    val enc = sampleTransactionInput.encode
    assert(enc.decode == sampleTransactionInput)
    val rendParse = txFromString(enc.rendered)
    assert(rendParse == enc)
    assert(rendParse.decode == sampleTransactionInput)
  }


}
