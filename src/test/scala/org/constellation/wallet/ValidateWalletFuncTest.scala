package org.constellation.wallet

import java.security.{KeyPair, PrivateKey, PublicKey}
import org.json4s.native.Serialization
import org.scalatest.FlatSpec

import constellation._
import org.constellation.crypto.KeyUtils._

case class SetSerialize(s: Set[String])

class ValidateWalletFuncTest extends FlatSpec {

  val kp: KeyPair = makeKeyPair()

  "Set serialize" should "test" in {

    import constellation._
    val s = SetSerialize(Set("a", "b", "c"))
    assert(s.json.x[SetSerialize] == s)

  }

  /*  "Wallet KeyStore" should "build a keystore properly" in {

    val file = new File("keystoretest.p12")
    val file2 = new File("keystoretest.bks")
    val pass = "fakepassword".toCharArray
    val (p12A, bksA) = makeWalletKeyStore(
      saveCertTo = Some(file),
      savePairsTo = Some(file2),
      password = pass,
      numECDSAKeys = 10
    )

    val p12 = KeyStore.getInstance("PKCS12", "BC")
    p12.load(new FileInputStream(file), pass)

    val bks = KeyStore.getInstance("BKS", "BC")
    bks.load(new FileInputStream(file2), pass)

    assert(p12A.getCertificate("test_cert") == p12.getCertificate("test_cert"))

    file.delete()
    file2.delete()

    // Put more tests in here.

  }*/

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
    //  implicit val formats: Formats = DefaultFormats +
//      new PublicKeySerializer + new PrivateKeySerializer + new KeyPairSerializer
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

    assert(kp.getPublic.hashCode() == deser3.getPublic.hashCode())
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
      Seq.fill(50) { thunk }.map { _.length }.distinct

    assert(fill(makeKeyPair().getPrivate.getEncoded) == List(144))
    assert(fill(makeKeyPair().getPublic.getEncoded) == List(88))

  }

  "Address maker" should "create valid address deterministically" in {
    val addr = publicKeyToAddressString(kp.getPublic)
    assert(addr.length > 10)
    assert(addr.toCharArray.distinct.length > 5)
    val addr2 = publicKeyToAddressString(kp.getPublic)
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
