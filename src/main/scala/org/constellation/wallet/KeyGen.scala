package org.constellation.wallet

import java.security.{KeyPair, KeyPairGenerator, SecureRandom}
import java.security.spec.ECGenParameterSpec

object KeyGen {

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

}
