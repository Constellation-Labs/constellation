package org.constellation.wallet

import org.bitcoinj.core.Sha256Hash
import org.constellation.Fixtures
import org.constellation.keytool.KeyUtils
import org.scalatest.{FreeSpec, Matchers}
import org.web3j.crypto.Hash
import io.circe.generic.auto._
import io.circe.syntax._

import scala.util.Try

class BIP44Test extends FreeSpec with Matchers {
  val seedCode = "yard impulse luxury drive today throw farm pepper survey wreck glass federal"
  val msg = "Message for signing".getBytes()
  val msgKeccak = Hash.sha3(msg) //NoteL Hash.sha3 nneded for bitcoinj

  "BIP44 wallet keys should validate for DAG signature scheme" in {
    val bip44 = new BIP44(seedCode)
    val dagSign = bip44.signData(msg)
    val isValid = KeyUtils.verifySignature(msg, dagSign)(bip44.getChildKeyPairOfDepth().getPublic)
    assert(isValid)
  }

  "BIP44 wallet keys should validate for DAG signature scheme using publickey recovered from id" in {
    val bip44 = new BIP44(seedCode)
    val dagSign = bip44.signData(msg)
    val publicKey = KeyUtils.hexToPublicKey("08dda015c42ea066a52d68e2ab2985b5ab255d3d0fd2b90363548cc74963b156e1a6aec5beb1a0c1df86025ffded1dba91afa87ecacdc6e32934421ab6c28d9e")
    val isValid = KeyUtils.verifySignature(msg, dagSign)(publicKey)
    assert(isValid)
  }

  "BTC signature scheme should work for DAG" in {
    val bip44 = new BIP44(seedCode)
    val dagSign = bip44.signData(msgKeccak)
    val isValid = KeyUtils.verifySignature(msgKeccak, dagSign)(bip44.getChildKeyPairOfDepth().getPublic)
    assert(isValid)
  }

  "BTC signature scheme should work for DAG using publickey recovered from id" in {
    val bip44 = new BIP44(seedCode)
    val dagSign = bip44.signData(msgKeccak)
    val publicKey = KeyUtils.hexToPublicKey("08dda015c42ea066a52d68e2ab2985b5ab255d3d0fd2b90363548cc74963b156e1a6aec5beb1a0c1df86025ffded1dba91afa87ecacdc6e32934421ab6c28d9e")
    val isValid = KeyUtils.verifySignature(msgKeccak, dagSign)(publicKey)
    assert(isValid)
  }

  "DAG signature scheme should work not for BTC" in {
    val bip44 = new BIP44(seedCode)
    val isBTCValid = Try {
      val childKeyObj = bip44.getDeterministicKeyOfDepth()
      val btcJSignature = childKeyObj.sign(Sha256Hash.wrap(msg))
      childKeyObj.verify(Sha256Hash.wrap(msg), btcJSignature)
    }
    assert(isBTCValid.isFailure)
  }

  "Expected public key should be generated from the given mnemonic and depth" in {
    val childIndex = 0
    val bip44 = new BIP44(seedCode, childIndex)
    val keyPair = bip44.getChildKeyPairOfDepth()
    val publicKey = keyPair.getPublic

    val expected = "08dda015c42ea066a52d68e2ab2985b5ab255d3d0fd2b90363548cc74963b156e1a6aec5beb1a0c1df86025ffded1dba91afa87ecacdc6e32934421ab6c28d9e"
    val result = KeyUtils.publicKeyToHex(publicKey)

    result shouldBe expected
  }

  "Expected private key should be generated from the given mnemonic and depth" in {
    val childIndex = 0
    val bip44 = new BIP44(seedCode, childIndex)
    val keyPair = bip44.getChildKeyPairOfDepth()
    val privateKey = keyPair.getPrivate

    val expected = "677c919bf7f5129c2fa245774e08f8c64be16bf6ef4d9d3fcad570c7a503d8cba00706052b8104000aa1440342000408dda015c42ea066a52d68e2ab2985b5ab255d3d0fd2b90363548cc74963b156e1a6aec5beb1a0c1df86025ffded1dba91afa87ecacdc6e32934421ab6c28d9e"
    val result = KeyUtils.privateKeyToHex(privateKey)

    result shouldBe expected
  }

  "Expected address should be generated from the given mnemonic and depth" in {
    val childIndex = 0
    val bip44 = new BIP44(seedCode, childIndex)
    val keyPair = bip44.getChildKeyPairOfDepth()
    val publicKey = keyPair.getPublic

    val expected = "DAG4EqbfJNSYZDDfs7AUzofotJzZXeRYgHaGZ6jQ"
    val result = KeyUtils.publicKeyToAddressString(publicKey)

    result shouldBe expected
  }
}
