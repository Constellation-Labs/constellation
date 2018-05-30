package org.constellation.crypto

import java.io.{ByteArrayInputStream, File}
import java.math.BigInteger
import java.security.cert.CertificateFactory
import java.security.spec.{ECGenParameterSpec, PKCS8EncodedKeySpec, X509EncodedKeySpec}
import java.security.{SecureRandom, _}
import java.util.{Base64, Date}

import org.constellation.util.EncodedPublicKey
import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, Formats, JObject}
import org.spongycastle.asn1.x500.X500NameBuilder
import org.spongycastle.asn1.x500.style.BCStyle
import org.spongycastle.asn1.x509.SubjectPublicKeyInfo
import org.spongycastle.cert.X509v1CertificateBuilder
import org.spongycastle.operator.jcajce.JcaContentSignerBuilder

object KeyUtils extends KeyUtilsExt

/**
  * Need to compare this to:
  * https://github.com/bitcoinj/bitcoinj/blob/master/core/src/main/java/org/bitcoinj/core/ECKey.java
  *
  * The implementation here is a lot simpler and from stackoverflow post linked below
  * but has differences. Same library dependency but needs to be checked
  *
  * BitcoinJ is stuck on Java 6 for some things, so if we use it, probably better to pull
  * out any relevant code rather than using as a dependency.
  *
  * Based on: http://www.bouncycastle.org/wiki/display/JA1/Elliptic+Curve+Key+Pair+Generation+and+Key+Factories
  * I think most of the BitcoinJ extra code is just overhead for customization.
  * Look at the `From Named Curves` section of above citation. Pretty consistent with stackoverflow code
  * and below implementation.
  *
  * Need to review: http://www.bouncycastle.org/wiki/display/JA1/Using+the+Bouncy+Castle+Provider%27s+ImplicitlyCA+Facility
  * for security policy implications.
  *
  */
trait KeyUtilsExt {

  def makeWalletKeyStore(
                          validityInDays: Int = 500000,
                          orgName: String = "test",
                          orgUnitName: String = "test",
                          localityName: String = "test",
                          password: Array[Char],
                          numECDSAKeys: Int = 1000,
                          certEntryName: String = "test_cert",
                          rsaEntryName: String = "test_rsa",
                          ecdsaEntryNamePrefix: String = "ecdsa",
                          saveCertTo: Option[File] = None,
                          savePairsTo: Option[File] = None,
                          sslKeySize: Int = 4096
                        ): (KeyStore, KeyStore) = {
    import java.security.Security
    // Note this requires JDK 8u151 +
    // https://stackoverflow.com/questions/6481627/java-security-illegal-key-size-or-default-parameters
    Security.setProperty("crypto.policy", "unlimited")
    // java.lang.SecurityException: JCE cannot authenticate the provider SC
    // Use BC for now, for some reason SC not working. Just google it theres a fix.
    val bcProvider = new org.bouncycastle.jce.provider.BouncyCastleProvider()

    val prov = bcProvider // makeProvider
    Security.insertProviderAt(prov, 1)
    val keyGen: KeyPairGenerator = KeyPairGenerator.getInstance("RSA", prov)
    keyGen.initialize(sslKeySize)
    val keyPair = keyGen.generateKeyPair
    val startDate = new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000)
    val endDate = new Date(System.currentTimeMillis() + validityInDays * 24 * 60 * 60 * 1000)

    val nameBuilder = new X500NameBuilder(BCStyle.INSTANCE)
    nameBuilder.addRDN(BCStyle.O,orgName)
    nameBuilder.addRDN(BCStyle.OU,orgUnitName)
    nameBuilder.addRDN(BCStyle.L,localityName)

    val x500Name = nameBuilder.build()
    val random = new SecureRandom()

    val subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic.getEncoded)
    val v1CertGen = new X509v1CertificateBuilder(
      x500Name, BigInteger.valueOf(random.nextLong()),startDate,endDate,x500Name,subjectPublicKeyInfo
    )

    val sigGen = new JcaContentSignerBuilder("SHA256WithRSAEncryption")
      .setProvider(prov).build(keyPair.getPrivate)

    val x509CertificateHolder = v1CertGen.build(sigGen)

    val certf = CertificateFactory.getInstance("X.509")
    val cert = certf.generateCertificate(new ByteArrayInputStream(x509CertificateHolder.getEncoded))

    // There's another solution here: https://stackoverflow.com/questions/13894699/java-how-to-store-a-key-in-keystore
    // Maybe worth looking at we'll see. This works for now.
    // Theres a lot of different keystores we actually need to support.
    // PKCS12 for the SSL certs (between nodes unless they get a proper cert from lets encrypt) is easy
    // BKS is also easy
    // There's a lot more and more details to get into. This is pretty complicated but this can serve
    // As an initial example to at least get the right classes in place.
    val ks: KeyStore = KeyStore.getInstance("PKCS12", bcProvider)
    ks.load(null, password)
    // https://stackoverflow.com/questions/6656263/why-do-i-get-the-error-cannot-store-non-privatekeys-when-creating-an-ssl-socke
    // ^ in case next step is confusing:

    import java.io.FileOutputStream
    import java.security.KeyStore
    val bks = KeyStore.getInstance("BKS", "BC")
    bks.load(null, null)

    ks.setCertificateEntry(certEntryName, cert)
    ks.setKeyEntry(rsaEntryName, keyPair.getPrivate, password, Array(cert))

    val ecdsaKeys = Seq.fill(numECDSAKeys){makeKeyPairFrom(provider = prov)}

    ecdsaKeys.zipWithIndex.foreach{
      case (k, i) =>
        bks.setCertificateEntry(certEntryName, cert)
        bks.setKeyEntry(rsaEntryName, keyPair.getPrivate, password, Array(cert))
        bks.setKeyEntry(s"${ecdsaEntryNamePrefix}_priv_" + i, k.getPrivate, password, Array(cert))
        bks.setKeyEntry(s"${ecdsaEntryNamePrefix}_pub_" + i, k.getPublic, password, Array(cert))
    }
    savePairsTo.foreach { file =>
      bks.store(new FileOutputStream(file), password)
    }

    saveCertTo.foreach { file =>
      ks.store(new FileOutputStream(file), password)
    }

    ks -> bks
  }

  /**
    * Simple Bitcoin like wallet grabbed from some stackoverflow post
    * Mostly for testing purposes, feel free to improve.
    * Source: https://stackoverflow.com/questions/29778852/how-to-create-ecdsa-keypair-256bit-for-bitcoin-curve-secp256k1-using-spongy
    * @return : Private / Public keys following BTC implementation
    */
  def makeKeyPair(secureRandom: SecureRandom = new SecureRandom()): KeyPair = {
    import java.security.Security
    Security.insertProviderAt(new org.spongycastle.jce.provider.BouncyCastleProvider(), 1)
    val keyGen: KeyPairGenerator = KeyPairGenerator.getInstance("ECDsA", "SC")
    val ecSpec = new ECGenParameterSpec("secp256k1")
    keyGen.initialize(ecSpec, secureRandom)
    keyGen.generateKeyPair
  }

  def makeKeyPairFrom(provider: Provider): KeyPair = {
    val keyGen: KeyPairGenerator = KeyPairGenerator.getInstance("ECDsA", provider)
    val ecSpec = new ECGenParameterSpec("secp256k1")
    keyGen.initialize(ecSpec, new SecureRandom())
    keyGen.generateKeyPair
  }

  // Utilities for getting around conversion errors / passing around parameters
  // through strange APIs that might take issue with your strings
  def base64(bytes: Array[Byte]): String = Base64.getEncoder.encodeToString(bytes)
  def fromBase64(b64Str: String): Array[Byte] = Base64.getDecoder.decode(b64Str)
  def base64FromBytes(bytes: Array[Byte]): String = new String(bytes)

  val DefaultSignFunc = "SHA512withECDSA"

  /**
    * https://stackoverflow.com/questions/31485517/verify-ecdsa-signature-using-spongycastle
    * https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html
    * https://bouncycastle.org/specifications.html
    * https://stackoverflow.com/questions/16662408/correct-way-to-sign-and-verify-signature-using-bouncycastle
    * @param bytes: Data to sign. Use text.toBytes or even better base64 encryption
    * @param signFunc: How to sign the data. There's a bunch of these,
    *                this needs to be made into an enum or something (instead of a val const),
    *                make sure if you fix it you make it consistent with json4s
    *                usages!
    * @param privKey: Java Private Key generated above with ECDSA
    * @return : Signature of bytes based on the text signed with the private key
    *         This can be checked by anyone to be equal to the input text with
    *         access only to the public key paired to the input private key! Fun
    */
  def signData(
                bytes: Array[Byte],
                signFunc: String = DefaultSignFunc
              )(implicit privKey: PrivateKey): Array[Byte] = {
    val signature = Signature.getInstance(signFunc, "SC")
    signature.initSign(privKey)
    signature.update(bytes)
    val signedOutput = signature.sign()
    signedOutput
  }

  /**
    * Verify a signature of some input text with a public key
    * This is called by verifier nodes checking to see if transactions are legit
    *
    * WARNING IF THIS FUNCTION IS MODIFIED BY AN ILLEGITIMATE NODE YOU WILL
    * BE BLACKLISTED FROM THE NETWORK. DO NOT MODIFY THIS FUNCTION UNLESS YOU
    * HAVE APPROVAL. OTHER NODES WILL CHECK YOUR VERIFICATIONS.
    *
    * YOU HAVE BEEN WARNED.
    *
    * @param originalInput: Byte input to verify, recommended that you
    *                         use base64 encoding if dealing with arbitrary text
    *                         meant to be shared over RPC / API protocols that
    *                         have issues with strange characters. If within same
    *                         JVM then just use text.getBytes (see unit tests for examples)
    * @param signedOutput: Byte array of output of calling signData method above
    * @param signFunc: Signature function to use. Use the default one for now.
    *                To be discussed elsewhere if revision necessary
    * @param pubKey: Public key to perform verification against.
    *              Only the public key which corresponds to the private key who
    *              performed the signing will verify properly
    * @return : True if the signature / transaction is legitimate.
    *         False means dishonest signer / fake transaction
    */
  def verifySignature(
                       originalInput: Array[Byte],
                       signedOutput: Array[Byte],
                       signFunc: String = DefaultSignFunc
                     )(implicit pubKey: PublicKey): Boolean = {
    val verifyingSignature = Signature.getInstance(signFunc, "SC")
    verifyingSignature.initVerify(pubKey)
    verifyingSignature.update(originalInput)
    val result = verifyingSignature.verify(signedOutput)
    result
  }

  // https://stackoverflow.com/questions/42651856/how-to-decode-rsa-public-keyin-java-from-a-text-view-in-android-studio
  def bytesToPublicKey(encodedBytes: Array[Byte]): PublicKey = {
    val spec = new X509EncodedKeySpec(encodedBytes)
    import java.security.KeyFactory
    val kf = KeyFactory.getInstance("ECDsA", "SC")
    kf.generatePublic(spec)
  }

  def bytesToPrivateKey(encodedBytes: Array[Byte]): PrivateKey = {
    val spec = new PKCS8EncodedKeySpec(encodedBytes)
    import java.security.KeyFactory
    val kf = KeyFactory.getInstance("ECDsA", "SC")
    kf.generatePrivate(spec)
  }

  // TODO : Use a more secure address function.
  // Couldn't find a quick dependency for this, TBI
  // https://en.bitcoin.it/wiki/Technical_background_of_version_1_Bitcoin_addresses
  def publicKeyToAddressString(
                          key: PublicKey
                        ): String = {
    import constellation.SHA256Ext
    base64(key.getEncoded).sha256.sha256
  }

  def publicKeysToAddressString(
                          key: Seq[PublicKey]
                        ): String = {
    import constellation.SHA256Ext
    key.map{z => base64(z.getEncoded)}.mkString.sha256.sha256
  }

  class PrivateKeySerializer extends CustomSerializer[PrivateKey](format => ( {
    case jObj: JObject =>
      implicit val f: Formats = format
      bytesToPrivateKey(fromBase64((jObj \ "key").extract[String]))
  }, {
    case key: PrivateKey =>
      JObject("key" -> JString(KeyUtils.base64(key.getEncoded)))
  }
  ))

  class PublicKeySerializer extends CustomSerializer[PublicKey](format => ( {
    case jstr: JObject =>
      implicit val f: Formats = format
      bytesToPublicKey(fromBase64((jstr \ "key").extract[String]))
  }, {
    case key: PublicKey =>
      JObject("key" -> JString(KeyUtils.base64(key.getEncoded)))
  }
  ))

  class KeyPairSerializer extends CustomSerializer[KeyPair](format => ( {
    case jObj: JObject =>
      implicit val f: Formats = format
      val pubKey = (jObj \ "publicKey").extract[PublicKey]
      val privKey = (jObj \ "privateKey").extract[PrivateKey]
      val kp = new KeyPair(pubKey, privKey)
      kp
  }, {
    case key: KeyPair =>
      implicit val f: Formats = format
      JObject(
        "publicKey" -> JObject("key" -> JString(KeyUtils.base64(key.getPublic.getEncoded))),
        "privateKey" -> JObject("key" -> JString(KeyUtils.base64(key.getPrivate.getEncoded)))
      )
  }
  ))

  implicit class PublicKeyExt(publicKey: PublicKey) {
    // Conflict with old schema, add later
    //  def address: Address = pubKeyToAddress(publicKey)
    def encoded: EncodedPublicKey = EncodedPublicKey(base64(publicKey.getEncoded))
  }


}

