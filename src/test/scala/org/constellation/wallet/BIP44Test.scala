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

  "Transaction should be signed correctly" in {
    val childIndex = 0
    val bip44 = new BIP44(seedCode, childIndex)
    val keyPair = bip44.getChildKeyPairOfDepth()
    val publicKey = keyPair.getPublic
    val privateKey = keyPair.getPrivate
    val address = KeyUtils.publicKeyToAddressString(publicKey)

    val src = address
    val dst = "DAG48nmxgpKhZzEzyc86y9oHotxxG57G8sBBwj56"
    val amount = 100000000L
    val prevHash = "08e6f0c3d65ed0b393604ffe282374bf501956ba447bc1c5ac49bcd2e8cc44fd"
    val ordinal = 567L
    val fee = 123456L
    val salt = 7370566588033602435L
    val lastTxRef = LastTransactionRef(prevHash, ordinal)
    val txData = TransactionEdgeData(amount = amount, lastTxRef = lastTxRef, fee = Option(fee), salt = salt)
    val oe = ObservationEdge(
      Seq(
        TypedEdgeHash(src, EdgeHashType.AddressHash),
        TypedEdgeHash(dst, EdgeHashType.AddressHash)
      ),
      TypedEdgeHash(txData.getEncoding, EdgeHashType.TransactionDataHash)
    )
    val runLengthEncoding = oe.getEncoding
    val soe = SignHelp.signedObservationEdge(oe)(keyPair)
    val tx = Transaction(edge = Edge(oe, soe, txData), lastTxRef = lastTxRef, isDummy = false, isTest = false)

//    val signature = bip44.signData(tx.edge.observationEdge.hash.getBytes())
//    val signDataManually = KeyUtils.bytes2hex(signature)

    println("mnemonic:              " + seedCode)
    println("bip44 path:            " + bip44.chainPathPrefix + childIndex)
    println("withPrefixPriKey:      " + KeyUtils.bytes2hex(privateKey.getEncoded))
    println("privateKey:            " + KeyUtils.privateKeyToHex(privateKey))
    println("withPrefixPubKey:      " + KeyUtils.bytes2hex(publicKey.getEncoded))
    println("publicKey:             " + KeyUtils.publicKeyToHex(publicKey))
    println("address:               " + address)
    println("transaction:           " + tx.asJson.noSpaces)
    println("hex encoded signature: " + tx.edge.signedObservationEdge.signatureBatch.signatures.head.signature)
    println("runLengthEncoding:     " + runLengthEncoding)
    println("runLenEnc sha256 hash  " + Hashable.hash(runLengthEncoding))
  }
}
