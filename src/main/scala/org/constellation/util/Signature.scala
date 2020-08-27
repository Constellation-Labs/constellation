package org.constellation.util

import java.security.{KeyPair, PublicKey}

import cats.kernel.Monoid
import constellation._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils._
import org.constellation.primitives.Schema._
import org.constellation.schema.Id

trait Signable {

  protected def toEncode: AnyRef = this

  def signInput: Array[Byte] = hash.getBytes()

  def serializeStringWithUTF8: Boolean = false

  def hash: String = hashSerialized(getEncoding, serializeStringWithUTF8)

  def short: String = hash.slice(0, 5)

  def getEncoding: String = hashSerialized(toEncode, serializeStringWithUTF8)

  def getHexEncoding = KeyUtils.bytes2hex(hashSerializedBytes(toEncode))

}

case class SingleHashSignature(hash: String, hashSignature: HashSignature) {

  def valid(expectedHash: String): Boolean =
    hash == expectedHash && hashSignature.valid(expectedHash)
}

object SingleHashSignature {
  implicit val singleHashSignatureEncoder: Encoder[SingleHashSignature] = deriveEncoder
  implicit val singleHashSignatureDecoder: Decoder[SingleHashSignature] = deriveDecoder
}

case class HashSignature(
  signature: String,
  id: Id
) extends Ordered[HashSignature] {

  def publicKey: PublicKey = id.toPublicKey

  def address: String = publicKey.address

  def valid(hash: String): Boolean =
    verifySignature(hash.getBytes(), KeyUtils.hex2bytes(signature))(publicKey)

  override def compare(that: HashSignature): Int =
    signature.compare(that.signature)
}

object HashSignature {
  implicit val hashSignatureEncoder: Encoder[HashSignature] = deriveEncoder
  implicit val hashSignatureDecoder: Decoder[HashSignature] = deriveDecoder
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
    val unique = newSignatures.groupBy(_.id).map { _._2.maxBy(_.signature) }.toSeq.sorted
    this.copy(
      signatures = unique
    )
  }

  def withSignature(hs: HashSignature): SignatureBatch = {
    val toAdd = Seq(hs)
    val newSignatures = (signatures ++ toAdd).distinct
    val unique = newSignatures.groupBy(_.id).map { _._2.maxBy(_.signature) }.toSeq.sorted
    this.copy(
      signatures = unique
    )
  }

}

object SignatureBatch {
  implicit val signatureBatchEncoder: Encoder[SignatureBatch] = deriveEncoder
  implicit val signatureBatchDecoder: Decoder[SignatureBatch] = deriveDecoder
}

trait SignHelpExt {

  def hashSign(hash: String, keyPair: KeyPair): HashSignature =
    HashSignature(
      signHashWithKey(hash, keyPair.getPrivate),
      keyPair.getPublic.toId
    )

  def hashSignBatchZeroTyped(productHash: Signable, keyPair: KeyPair): SignatureBatch = {
    val hash = productHash.hash
    SignatureBatch(hash, Seq(hashSign(hash, keyPair)))
  }

  def signedObservationEdge(oe: ObservationEdge)(implicit kp: KeyPair): SignedObservationEdge =
    SignedObservationEdge(hashSignBatchZeroTyped(oe, kp))

}

object SignHelp extends SignHelpExt
