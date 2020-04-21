package org.constellation.wallet
import org.bitcoinj.core.Sha256Hash
import org.constellation.keytool.KeyUtils
import org.scalatest.{FreeSpec, Matchers}
import org.web3j.crypto.Hash

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

  "BTC signature scheme should work for DAG" in {
    val bip44 = new BIP44(seedCode)
    val dagSign = bip44.signData(msgKeccak)
    val isValid = KeyUtils.verifySignature(msgKeccak, dagSign)(bip44.getChildKeyPairOfDepth().getPublic)
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
}
