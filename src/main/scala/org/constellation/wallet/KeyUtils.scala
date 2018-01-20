package org.constellation.wallet

import java.security._
import java.security.spec.ECGenParameterSpec
import java.util.Base64

object KeyUtils {

  /**
    * Simple Bitcoin like wallet grabbed from some stackoverflow post
    * Mostly for testing purposes, feel free to improve.
    * Source: https://stackoverflow.com/questions/29778852/how-to-create-ecdsa-keypair-256bit-for-bitcoin-curve-secp256k1-using-spongy
    * @return : Private / Public keys following BTC implementation
    */
  def makeKeyPair(): KeyPair = {
    import java.security.Security
    Security.insertProviderAt(new org.spongycastle.jce.provider.BouncyCastleProvider(), 1)
    val keyGen: KeyPairGenerator = KeyPairGenerator.getInstance("ECDsA", "SC")
    val ecSpec = new ECGenParameterSpec("secp256k1")
    keyGen.initialize(ecSpec, new SecureRandom())
    keyGen.generateKeyPair
  }

  // Utilities for getting around conversion errors / passing around parameters
  // through strange APIs that might take issue with your strings
  def base64(bytes: Array[Byte]): String = Base64.getEncoder.encodeToString(bytes)
  def fromBase64(b64Str: String): Array[Byte] = Base64.getDecoder.decode(b64Str)

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

}
