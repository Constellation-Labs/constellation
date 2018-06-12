package org.constellation.util

import java.security.{KeyPair, PrivateKey, PublicKey}

import constellation._
import org.constellation.crypto.Base58
import org.constellation.primitives.Schema.Id

object POW extends POWExt

trait POWExt {

  def proofOfWork(input: String, difficulty: Option[Int]): String = {
    var done = false
    var count = 0L
    while (!done) {
      count += 1
      if (verifyPOW(input, count.toString, difficulty)) {
        done = true
      }
    }
    count.toString
  }

  def hashNonce(input: String, nonce: String): String = {
    val strNonced = input + nonce
    strNonced.sha256.sha256
  }

  def verifyPOW(input: String, nonce: String, difficulty: Option[Int]): Boolean = {
    val sha = hashNonce(input, nonce)
    difficulty.exists{d => sha.startsWith("0"*d)}
  }

}

trait ProductHash extends Product {

  def signInput: Array[Byte] = hash.getBytes()
  def hash: String = productSeq.json.sha256
  def short: String = hash.slice(0, 5)
  def signKeys(privateKeys: Seq[PrivateKey]): Seq[String] = privateKeys.map { pk => base64(signData(signInput)(pk)) }
  def powInput(signatures: Seq[String]): String = (productSeq ++ signatures).json
  def pow(signatures: Seq[String], difficulty: Int): String = POW.proofOfWork(powInput(signatures), Some(difficulty))
  def productSeq: Seq[Any] = this.productIterator.toArray.toSeq

}

/*
Option = SignableData
None = Unsigned
Some = signed
 */

case class EncodedPublicKey(b58Encoded: String) {
  def toPublicKey: PublicKey = bytesToPublicKey(Base58.decode(b58Encoded))
}

// TODO: Move POW to separate class for rate liming.
// Add flatten method? To allow merging of multiple signed datas -- need to preserve hashes by stack depth though.
// i.e. two people signing one transaction is different from one person signing it, and then another signing
// the signed transaction.
// TODO: Extend to monad, i.e. SignedData extends SignableData vs UnsignedData
case class Signed[T <: ProductHash](
                                     data: T,
                                     time: Long,
                                     //        startTime: Long,
                                     //        endTime: Long,
                                     //        nonce: String,
                                     //        difficulty: Int,
                                     encodedPublicKeys: Seq[EncodedPublicKey],
                                     signatures: Seq[String]
                                        ) extends ProductHash {
  def publicKeys: Seq[PublicKey] = encodedPublicKeys.map{_.toPublicKey}
  def id: Id = Id(publicKeys.head)
  def validSignatures: Boolean = signatures.zip(encodedPublicKeys).forall{ case (sig, pubEncoded) =>
    val pub = pubEncoded.toPublicKey
    verifySignature(data.signInput, fromBase64(sig))(pub) && signatures.nonEmpty && encodedPublicKeys.nonEmpty
  }
 // def validPOW: Boolean = POW.verifyPOW(data.powInput(signatures), nonce, Some(difficulty))
  def valid: Boolean = validSignatures && time > minimumTime
  // && validPOW && startTime > minimumTime && endTime > minimumTime

}

trait POWSignHelp {

  implicit class LinearHashHelpers[T <: ProductHash](t: T) {
    def sign2(keyPairs: Seq[KeyPair], difficulty: Int = 1): Signed[T] =
      signPairs[T](t, keyPairs, difficulty)
    def signed(difficulty: Int = 0)(implicit keyPair: KeyPair): Signed[T] = signPairs(t, Seq(keyPair), difficulty)
    def multiSigned(difficulty: Int = 0)(implicit keyPairs: Seq[KeyPair]): Signed[T] = signPairs(t, keyPairs, difficulty)
  }

  def signPairs[T <: ProductHash](
                                     t: T,
                                     keyPairs: Seq[KeyPair],
                                     difficulty: Int = 0
                                   ): Signed[T] = {
    val startTime = System.currentTimeMillis()
    val signatures = t.signKeys(keyPairs.map{_.getPrivate})
  //  val nonce = t.pow(signatures, difficulty)
  //  val endTime = System.currentTimeMillis()
    Signed(
      t, startTime, keyPairs.map{_.getPublic.encoded}, signatures
    )
  }

}
