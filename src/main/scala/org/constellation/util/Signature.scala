package org.constellation.util

import java.security.{KeyPair, PublicKey}

import cats.kernel.Monoid
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils._
import org.constellation.primitives.{Edge, Schema, Transaction}
import org.constellation.primitives.Schema._

/** Documentation. */
trait Signable {

  /** Documentation. */
  def signInput: Array[Byte] = hash.getBytes()

  /** Documentation. */
  def hash: String = this.kryo.sha256

  /** Documentation. */
  def short: String = hash.slice(0, 5)

}

/** Documentation. */
case class SingleHashSignature(hash: String, hashSignature: HashSignature) {

  /** Documentation. */
  def valid: Boolean = hashSignature.valid(hash)
}

/** Documentation. */
case class HashSignature(
                          signature: String,
                          id: Id
                        ) extends Ordered[HashSignature] {

  /** Documentation. */
  def publicKey: PublicKey = id.toPublicKey

  /** Documentation. */
  def address: String = publicKey.address

  /** Documentation. */
  def valid(hash: String): Boolean =
    verifySignature(hash.getBytes(), KeyUtils.hex2bytes(signature))(publicKey)

  /** Documentation. */
  override def compare(that: HashSignature): Int = {
    signature compare that.signature
  }
}

/** Documentation. */
case class SignatureBatch(
                           hash: String,
                           signatures: Seq[HashSignature]
                         ) extends Monoid[SignatureBatch] {

  /** Documentation. */
  def valid: Boolean = {
    signatures.forall(_.valid(hash))
  }

  /** Documentation. */
  override def empty: SignatureBatch = SignatureBatch(hash, Seq())

  // This is unsafe

  /** Documentation. */
  override def combine(x: SignatureBatch, y: SignatureBatch): SignatureBatch =
    x.copy(signatures = (x.signatures ++ y.signatures).distinct.sorted)

  /** Documentation. */
  def withSignatureFrom(other: KeyPair): SignatureBatch = {
    withSignature(hashSign(hash, other))
  }

  /** Documentation. */
  def plus(other: SignatureBatch): SignatureBatch = {
    val toAdd = other.signatures
    val newSignatures = (signatures ++ toAdd).distinct
    val unique = newSignatures.groupBy(_.id).map{_._2.maxBy(_.signature)}.toSeq.sorted
    this.copy(
      signatures = unique
    )
  }

  /** Documentation. */
  def withSignature(hs: HashSignature): SignatureBatch = {
    val toAdd = Seq(hs)
    val newSignatures = (signatures ++ toAdd).distinct
    val unique = newSignatures.groupBy(_.id).map{_._2.maxBy(_.signature)}.toSeq.sorted
    this.copy(
      signatures = unique
    )
  }

}

/** Documentation. */
trait SignHelpExt {

  /** Documentation. */
  def hashSign(hash: String, keyPair: KeyPair): HashSignature = {
    HashSignature(
      signHashWithKey(hash, keyPair.getPrivate),
      keyPair.getPublic.toId
    )
  }

  /** Documentation. */
  def hashSignBatchZeroTyped(productHash: Signable, keyPair: KeyPair): SignatureBatch = {
    val hash = productHash.hash
    SignatureBatch(hash, Seq(hashSign(hash, keyPair)))
  }

  /** Documentation. */
  def signedObservationEdge(oe: ObservationEdge)(implicit kp: KeyPair): SignedObservationEdge = {
    SignedObservationEdge(hashSignBatchZeroTyped(oe, kp))
  }

  /**
    * Transaction builder (for local use)
    * @param src : Source address
    * @param dst : Destination address
    * @param amount : Quantity
    * @param keyPair : Signing pair matching source
    * @param normalized : Whether quantity is normalized by NormalizationFactor (1e-8)
    * @return : Resolved transaction in edge format
    */

  /** Documentation. */
  def createTransaction(src: String,
                        dst: String,
                        amount: Long,
                        keyPair: KeyPair,
                        normalized: Boolean = true
                       ): Transaction = {
    val amountToUse = if (normalized) amount * Schema.NormalizationFactor else amount

    val txData = TransactionEdgeData(amountToUse)

    val oe = ObservationEdge(
      Seq(
        TypedEdgeHash(src, EdgeHashType.AddressHash),
        TypedEdgeHash(dst, EdgeHashType.AddressHash)
      ),
      TypedEdgeHash(txData.hash, EdgeHashType.TransactionDataHash)
    )

    val soe = signedObservationEdge(oe)(keyPair)

    Transaction(Edge(oe, soe, txData))
  }

}

/** Documentation. */
object SignHelp extends SignHelpExt
