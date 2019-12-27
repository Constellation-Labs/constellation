package org.constellation.util

import java.security.{KeyPair, PublicKey}

import cats.kernel.Monoid
import constellation._
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils._
import org.constellation.primitives.Schema._

trait Signable {

  def signInput: Array[Byte] = hash.getBytes()

  def hash: String = this.kryo.sha256

  def short: String = hash.slice(0, 5)

}

case class SingleHashSignature(hash: String, hashSignature: HashSignature) {

  def valid: Boolean = hashSignature.valid(hash)
}

case class HashSignature(
  signature: String,
  pubKeyHex: String,
  address: String
) extends Ordered[HashSignature] {

  def publicKey: PublicKey = KeyUtils.hexToPublicKey(pubKeyHex)

  def valid(hash: String): Boolean = verifySignature(hash.getBytes(), KeyUtils.hex2bytes(signature))(publicKey)

//  {
//    val valid = verifySignature(hash.getBytes(), KeyUtils.hex2bytes(signature))(publicKey)
//    println("HashSignature valid: " + valid)
//    valid
//  }


  override def compare(that: HashSignature): Int =
    signature.compare(that.signature)
}

case class SignatureBatch(
  hash: String,
  signatures: Seq[HashSignature]
) extends Monoid[SignatureBatch] {

  def valid: Boolean =
    signatures.forall(_.valid(hash))

  override def empty: SignatureBatch = SignatureBatch(hash, Seq())

  // This is unsafe

  override def combine(x: SignatureBatch, y: SignatureBatch): SignatureBatch =
    x.copy(signatures = (x.signatures ++ y.signatures).distinct.sorted)

  def withSignatureFrom(other: KeyPair): SignatureBatch =
    withSignature(hashSign(hash, other))

  def plus(other: SignatureBatch): SignatureBatch = {
    val toAdd = other.signatures
    val newSignatures = (signatures ++ toAdd).distinct
    val unique = newSignatures.groupBy(_.address).map { _._2.maxBy(_.signature) }.toSeq.sorted
    this.copy(
      signatures = unique
    )
  }

  def withSignature(hs: HashSignature): SignatureBatch = {
    val toAdd = Seq(hs)
    val newSignatures = (signatures ++ toAdd).distinct
    val unique = newSignatures.groupBy(_.address).map { _._2.maxBy(_.signature) }.toSeq.sorted
    this.copy(
      signatures = unique
    )
  }

}

trait SignHelpExt {

  def hashSign(hash: String, keyPair: KeyPair): HashSignature =
    HashSignature(
      signHashWithKey(hash, keyPair.getPrivate),
      KeyUtils.publicKeyToHex(keyPair.getPublic),
      KeyUtils.publicKeyToAddressString(keyPair.getPublic)
    )

  def hashSignBatchZeroTyped(productHash: Signable, keyPair: KeyPair): SignatureBatch = {
    val hash = productHash.hash
    SignatureBatch(hash, Seq(hashSign(hash, keyPair)))
  }

  def signedObservationEdge(oe: ObservationEdge)(implicit kp: KeyPair): SignedObservationEdge =
    SignedObservationEdge(hashSignBatchZeroTyped(oe, kp))

}

object SignHelp extends SignHelpExt
