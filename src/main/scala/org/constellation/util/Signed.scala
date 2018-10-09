package org.constellation.util

import java.security.{KeyPair, PrivateKey, PublicKey}

import cats.kernel.Monoid
import constellation._
import org.constellation.crypto.Base58
import org.constellation.primitives.Schema
import org.constellation.primitives.Schema._

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
  def bundleHash = BundleHash(hash)
  def short: String = hash.slice(0, 5)
  def signKeys(privateKeys: Seq[PrivateKey]): Seq[String] = privateKeys.map { pk => base64(signData(signInput)(pk)) }
  def signKey(privateKey: PrivateKey): String = base64(signData(signInput)(privateKey))
  def powInput(signatures: Seq[String]): String = (productSeq ++ signatures).json
  def pow(signatures: Seq[String], difficulty: Int): String = POW.proofOfWork(powInput(signatures), Some(difficulty))
  def productSeq: Seq[Any] = this.productIterator.toArray.toSeq

}

case class HashSignature(signature: String,
                         b58EncodedPublicKey: String) extends Ordered[HashSignature] {
  def publicKey: PublicKey = EncodedPublicKey(b58EncodedPublicKey).toPublicKey
  def address: String = publicKey.address
  def toId: Id = Id(EncodedPublicKey(b58EncodedPublicKey))
  def valid(hash: String): Boolean =
    verifySignature(hash.getBytes(), fromBase64(signature))(publicKey)

  override def compare(that: HashSignature): Int = {
    signature compare that.signature
  }
}

case class SignatureBatch(
                         hash: String,
                         signatures: Seq[HashSignature]
                         ) extends Monoid[SignatureBatch] {

  def valid: Boolean = {
    signatures.forall(_.valid(hash))
  }

  def plus(other: SignatureBatch): SignatureBatch = {
    this.copy(
      signatures = (signatures ++ other.signatures).distinct.sorted
    )
  }
  def plus(other: KeyPair): SignatureBatch = {
    this.copy(
      signatures = (signatures :+ hashSign(hash, other)).distinct.sorted
    )
  }

  override def empty: SignatureBatch = SignatureBatch(hash, Seq())

  override def combine(x: SignatureBatch, y: SignatureBatch): SignatureBatch =
    x.copy(signatures = (x.signatures ++ y.signatures).distinct.sorted)

}

case class EncodedPublicKey(b58Encoded: String) {
  def toPublicKey: PublicKey = bytesToPublicKey(Base58.decode(b58Encoded))
}

// TODO: Move POW to separate class for rate liming.
// Add flatten method? To allow merging of multiple signed datas -- need to preserve hashes by stack depth though.
// i.e. two people signing one transaction is different from one person signing it, and then another signing
// the signed transaction.
// TODO: Extend to monad, i.e. SignedData extends SignableData vs UnsignedData
case class Signed[T <: ProductHash](data: T,
                                    time: Long,
                                    encodedPublicKeys: Seq[EncodedPublicKey],
                                    signatures: Seq[String]) extends ProductHash {
  def publicKeys: Seq[PublicKey] = encodedPublicKeys.map{_.toPublicKey}

  def id: Id = Id(publicKeys.head.encoded)

  def validSignatures: Boolean = signatures.zip(encodedPublicKeys).forall{ case (sig, pubEncoded) =>
    val pub = pubEncoded.toPublicKey
    val validS = verifySignature(data.signInput, fromBase64(sig))(pub) && signatures.nonEmpty && encodedPublicKeys.nonEmpty
    println(s"validS $validS")
    println(s"hash ${data.hash}")
    println(s"sign input ${data.signInput.toSeq}")
    println(s"fromb64 ${fromBase64(sig).toSeq}")
    println(s"pub $pub")
    println(s"json ${data.json}")
    validS
  }

  def valid: Boolean = validSignatures

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

  def hashSign(hash: String, keyPair: KeyPair): HashSignature = {
    HashSignature(
      signHashWithKeyB64(hash, keyPair.getPrivate),
      keyPair.getPublic.encoded.b58Encoded
    )
  }

  def hashSignBatchZeroTyped(hash: ProductHash, keyPair: KeyPair): SignatureBatch = {
    SignatureBatch(hash.hash, Seq(hashSign(hash.hash, keyPair)))
  }

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
  def createTransaction(src: String,
                        dst: String,
                        amount: Long,
                        keyPair: KeyPair,
                        normalized: Boolean = true): Transaction = {
    val amountToUse = if (normalized) amount * Schema.NormalizationFactor else amount

    val txData = TransactionEdgeData(amountToUse)
    val dataHash = Some(TypedEdgeHash(txData.hash, EdgeHashType.TransactionDataHash))

    val oe = ObservationEdge(
      TypedEdgeHash(src, EdgeHashType.AddressHash),
      TypedEdgeHash(dst, EdgeHashType.AddressHash),
      data = dataHash
    )

    val soe = signedObservationEdge(oe)(keyPair)

    Transaction(Edge(oe, soe, ResolvedObservationEdge(Address(src), Address(dst), Some(txData))))
  }

}

object SignHelp extends POWSignHelp