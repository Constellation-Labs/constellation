package org.constellation

import com.typesafe.scalalogging.Logger
import org.constellation.consensus.Consensus.RemoteMessage
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils._
import org.constellation.primitives.Schema._
import org.constellation.util.{ProductHash, Signed}
import org.scalatest.FlatSpec

case class TestSignable(a: String, b: Int) extends ProductHash
case class TestSignabledWrapper(testSignable: Signed[TestSignable]) extends RemoteMessage

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

  "Kryo" should "not interfere with json" in {

    /* TODO
    val kp = makeKeyPair()

    val tx = createTransactionSafe("a", "b", 1L, kp)
    val grp = KryoSerializer.serializeGrouped(tx)

    val hs = HandShakeMessage(HandShake(
      Peer(Id(kp.getPublic.encoded), Some(Fixtures.address), Some(Fixtures.address), Seq(), "").signed()(kp), Fixtures.address
    ).signed()(kp))

    val j1 = hs.json
    println(j1)
    val hs2 = hs.copy()

    // This is the craziest behavior ever. Turn the line on below and this test will break
    //val grp2 = KryoSerializer.serializeGrouped(hs2)
    val j2 = hs.json
    println(j2)
    assert(j1 == j2)
    */

  }

  "Handshake valid signature" should "test signing nested" in {

    val kp = makeKeyPair()

    val hs = HandShakeMessage(HandShake(
      Peer(Id(kp.getPublic.encoded), Some(Fixtures.address), Some(Fixtures.address), Seq(), "").signed()(kp), Fixtures.address
    ).signed()(kp))

    assert(hs.handShake.validSignatures)
    //assert(hs.handShake.validSignatures)
    logger.debug(hs.handShake.encodedPublicKeys.toString)


 //   val grp2 = KryoSerializer.serializeGrouped(hs)
    // val res2 = KryoSerializer.deserializeGrouped(grp2.toList).asInstanceOf[HandShakeMessage]
    /*assert(hs == res2)
    assert(hs.handShake.signatures == res2.handShake.signatures)
    assert(hs.handShake.publicKeys == res2.handShake.publicKeys)
    assert(hs.handShake.signInput.toSeq == res2.handShake.signInput.toSeq)
    assert(fromBase64(res2.handShake.signatures.head).toSeq == fromBase64(hs.handShake.signatures.head).toSeq)
*/
//    println(hs.handShake.data.hash)

    assert(
      hs.handShake.signatures.zip(hs.handShake.encodedPublicKeys).forall{ case (sig, pubEncoded) =>
      import hs.handShake.{logger => _, _}
      val pub = pubEncoded.toPublicKey
      val validS = verifySignature(data.signInput, fromBase64(sig))(pub) && signatures.nonEmpty && encodedPublicKeys.nonEmpty
      logger.debug(s"validS $validS")
      logger.debug(s"hash ${data.hash}")
      logger.debug(s"sign input ${data.signInput.toSeq}")
      logger.debug(s"fromb64 ${fromBase64(sig).toSeq}")
      logger.debug(s"pub $pub")
      logger.debug(s"json ${data.json}")
      validS
    })

    /*

    assert(
      verifySignature(hs.handShake.signInput, fromBase64(hs.handShake.signatures.head))(hs.handShake.publicKeys.head)
    )
*/

    //assert(res2.handShake.validSignatures)

    /*
        verifySignature(data.signInput, fromBase64(sig))(pub) && signatures.nonEmpty && encodedPublicKeys.nonEmpty

     */



  }


}
