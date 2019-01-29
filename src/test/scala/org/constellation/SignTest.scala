package org.constellation

import com.typesafe.scalalogging.Logger
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils._
import org.constellation.primitives.Schema._
import org.constellation.util.{ProductHash, Signed}
import org.scalatest.FlatSpec

case class TestSignable(a: String, b: Int) extends ProductHash
case class TestSignabledWrapper(testSignable: Signed[TestSignable])

import constellation._

class SignTest extends FlatSpec {
  
  val logger = Logger("SignTest")


  "Hashing" should "should work on test data" in {

    val input = "Input"
    val diff = Some(1)
    val nonce = proofOfWork(input, diff)
    val hash = hashNonce(input, nonce)
    assert(hash.startsWith("0"))
    assert(hash.length > 10)
    assert(nonce.nonEmpty)
    assert(verifyPOW(input, nonce, diff))

  }

  "Simple Sign" should "sign and hash a simple fake case class properly" in {

    val kp = KeyUtils.makeKeyPair()
    val data = TestSignable("a", 1)
    (0 to 2).foreach { d =>
      val powSigned = signPairs(data, Seq(kp), d)
      assert(powSigned.data == data)
      // println(powSigned.nonce)
      assert(powSigned.validSignatures)
      // assert(powSigned.validPOW)
      assert(powSigned.valid)
    }

    val signed = data.signed()(kp)
    assert(signed.validSignatures)

    /* TODO
    val tx = createTransactionSafe("a", "b", 1L, kp)
    val grp = KryoSerializer.serializeGrouped(tx)
    val res = KryoSerializer.deserializeGrouped(grp.toList).asInstanceOf[Transaction]
    assert(res.txData.validSignatures)
    */

  }

}
