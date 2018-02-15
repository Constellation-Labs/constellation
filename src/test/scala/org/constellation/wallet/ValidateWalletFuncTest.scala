package org.constellation.wallet


import java.io.File
import java.security.{KeyPair, PrivateKey, PublicKey}

import org.scalatest.FlatSpec
import KeyUtils._
import org.json4s.{DefaultFormats, Formats, NoTypeHints}
import org.json4s.native.Serialization

class ValidateWalletFuncTest  extends FlatSpec {

  val kp: KeyPair = makeKeyPair()

  "Wallet KeyStore" should "build a keystore properly" in {

    val tmp = new File("tmp")
    tmp.mkdir
    val file = new File("tmp", "keystoretest.p12")
    val file2 = new File("tmp", "keystoretest.bks")
    val res = makeWalletKeyStore(
      saveCertTo = Some(tmp),
      savePairsTo = Some(file2),
      password = "fakepassword".toCharArray,
      numECDSAKeys = 10
    )

    file.delete()
    file2.delete()
    tmp.delete()

    // Put more tests in here.

  }

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

  "KeyPair JSON" should "serialize to json4s using custom serializer" in {
    implicit val formats: Formats = DefaultFormats +
      new PublicKeySerializer + new PrivateKeySerializer + new KeyPairSerializer
    val ser = Serialization.write(kp.getPublic)
    val deser = Serialization.read[PublicKey](ser)
    assert(deser == kp.getPublic)
    val ser2 = Serialization.write(kp.getPrivate)
    val deser2 = Serialization.read[PrivateKey](ser2)
    assert(deser2 == kp.getPrivate)
    val ser3 = Serialization.write(kp)
    val deser3 = Serialization.read[KeyPair](ser3)
    assert(deser3.getPrivate == kp.getPrivate)
    assert(deser3.getPublic == kp.getPublic)

    assert(kp.getPrivate.getAlgorithm == deser3.getPrivate.getAlgorithm)
    assert(kp.getPublic.getAlgorithm == deser3.getPublic.getAlgorithm)
    assert(kp.getPublic.getFormat == deser3.getPublic.getFormat)
    assert(kp.getPrivate.getFormat == deser3.getPrivate.getFormat)

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

  "Address maker" should "create valid address deterministically" in {
    val addr = publicKeyToAddress(kp.getPublic)
    assert(addr.length > 10)
    assert(addr.toCharArray.distinct.length > 5)
    val addr2 = publicKeyToAddress(kp.getPublic)
    assert(addr == addr2)
  }

  "Key Encoding" should "verify keys can be encoded and decoded with X509/PKCS8 spec" in {
    val pub1 = kp.getPublic
    val priv1 = kp.getPrivate

    val encodedBytesPub = pub1.getEncoded
    val pub2 = bytesToPublicKey(encodedBytesPub)
    assert(pub1 == pub2)
    assert(pub1.getEncoded.sameElements(pub2.getEncoded))

    val encodedBytesPriv = priv1.getEncoded
    val priv2 = bytesToPrivateKey(encodedBytesPriv)
    assert(priv1 == priv2)
    assert(priv1.getEncoded.sameElements(priv2.getEncoded))
  }

}
