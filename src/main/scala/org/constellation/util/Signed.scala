package org.constellation.util

import java.security.{KeyPair, PrivateKey, PublicKey}
import cats.kernel.Monoid
import com.typesafe.scalalogging.Logger

import constellation._
import org.constellation.crypto.Base58
import org.constellation.crypto.KeyUtils._
import org.constellation.primitives.Schema
import org.constellation.primitives.Schema._

/** Proof-of-work object. */
object POW extends POWExt

/** Proof-of-work trait. */
trait POWExt {

  /** Returns the count string obtained from iteratively calling verifyPOW. */
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

  /** Double SHA256-hashes the input string, including a nonce. */
  def hashNonce(input: String, nonce: String): String = {
    val strNonced = input + nonce
    strNonced.sha256.sha256
  }

  /** Verify the nounce fulfilling the proof of work for the input string. */
  def verifyPOW(input: String, nonce: String, difficulty: Option[Int]): Boolean = {
    val sha = hashNonce(input, nonce)
    difficulty.exists { d => sha.startsWith("0" * d) }
  }

} // end trait POWExt

/** Product hash trait. */
trait ProductHash extends Product {

  /** ?? */
  def signInput: Array[Byte] = hash.getBytes()

  /** @return Hash of productSeq. */
  def hash: String = productSeq.json.sha256

  /** @return BundleHash wrapped hash. */
  def bundleHash = BundleHash(hash)

  /** @return The first 5 characters of the hash. */
  def short: String = hash.slice(0, 5)

  // doc
  def signKeys(privateKeys: Seq[PrivateKey]): Seq[String] = privateKeys.map { pk => base64(signData(signInput)(pk)) }

  // doc
  def signKey(privateKey: PrivateKey): String = base64(signData(signInput)(privateKey))

  /** @return JSON of concatenation of productSeq and input signatures. */
  def powInput(signatures: Seq[String]): String = (productSeq ++ signatures).json

  /** Performs proof-of-work ??. */
  def pow(signatures: Seq[String], difficulty: Int): String = POW.proofOfWork(powInput(signatures), Some(difficulty))

  // doc
  def productSeq: Seq[Any] = this.productIterator.toArray.toSeq

} // end trait ProductHash

/** Hash signature class. */
case class SingleHashSignature(hash: String, hashSignature: HashSignature) {

  /** @return Whether inputs are valid ??. */
  def valid: Boolean = hashSignature.valid(hash)

}

// doc
case class HashSignature(signature: String,
                         b58EncodedPublicKey: String) extends Ordered[HashSignature] {

  /** @returns Public key. */
  def publicKey: PublicKey = EncodedPublicKey(b58EncodedPublicKey).toPublicKey

  /** @returns The address string associated with the public key. */
  def address: String = publicKey.address

  /** @returns The ID associated with the public key. */
  def toId: Id = Id(EncodedPublicKey(b58EncodedPublicKey))

  /** @return Whether inputs are valid ??. */
  def valid(hash: String): Boolean =
    verifySignature(hash.getBytes(), fromBase64(signature))(publicKey)

  /** Compare this with the input signature. */
  override def compare(that: HashSignature): Int = {
    signature compare that.signature
  }

} // end case class HashSignature

// doc
case class SignatureBatch(
                           hash: String,
                           signatures: Seq[HashSignature]
                         ) extends Monoid[SignatureBatch] {

  /** @return Whether inputs are valid ??. */
  def valid: Boolean = {
    signatures.forall(_.valid(hash))
  }

  // doc
  override def empty: SignatureBatch = SignatureBatch(hash, Seq())

  // doc
  override def combine(x: SignatureBatch, y: SignatureBatch): SignatureBatch =
    x.copy(signatures = (x.signatures ++ y.signatures).distinct.sorted)

  // doc
  def plus(other: KeyPair): SignatureBatch = {
    plus(hashSign(hash, other))
  }

  // doc
  def plus(other: SignatureBatch): SignatureBatch = {
    val toAdd = other.signatures
    val newSignatures = (signatures ++ toAdd).distinct
    val unique = newSignatures.groupBy(_.b58EncodedPublicKey).map {
      _._2.maxBy(_.signature)
    }.toSeq.sorted
    this.copy(
      signatures = unique
    )
  }

  // doc
  def plus(hs: HashSignature): SignatureBatch = {
    val toAdd = Seq(hs)
    val newSignatures = (signatures ++ toAdd).distinct
    val unique = newSignatures.groupBy(_.b58EncodedPublicKey).map {
      _._2.maxBy(_.signature)
    }.toSeq.sorted
    this.copy(
      signatures = unique
    )
  }

} // end case class SignatureBatch

/** Wrapper for a public key ??. */
case class EncodedPublicKey(b58Encoded: String) {

  /** @returns The decoded public key. */
  def toPublicKey: PublicKey = bytesToPublicKey(Base58.decode(b58Encoded))

  /** @returns Id of this encoded public key. */
  def toId = Id(this)

}

// TODO: Move POW to separate class for rate liming.
// Add flatten method? To allow merging of multiple signed datas -- need to preserve hashes by stack depth though.
// i.e. two people signing one transaction is different from one person signing it, and then another signing
// the signed transaction.
// TODO: Extend to monad, i.e. SignedData extends SignableData vs UnsignedData

// doc
case class Signed[T <: ProductHash](data: T,
                                    time: Long,
                                    encodedPublicKeys: Seq[EncodedPublicKey],
                                    signatures: Seq[String]) extends ProductHash {

  val logger = Logger("Signed")

  // doc
  def publicKeys: Seq[PublicKey] = encodedPublicKeys.map {
    _.toPublicKey
  }

  // doc
  def id: Id = Id(publicKeys.head.encoded)

  /** @returns Wether signatures are valid. */
  def validSignatures: Boolean = signatures.zip(encodedPublicKeys).forall { case (sig, pubEncoded) =>
    val pub = pubEncoded.toPublicKey
    val validS = verifySignature(data.signInput, fromBase64(sig))(pub) && signatures.nonEmpty && encodedPublicKeys.nonEmpty
    logger.debug(s"validS $validS")
    logger.debug(s"hash ${data.hash}")
    logger.debug(s"sign input ${data.signInput.toSeq}")
    logger.debug(s"fromb64 ${fromBase64(sig).toSeq}")
    logger.debug(s"pub $pub")
    logger.debug(s"json ${data.json}")
    validS
  }

  /** @returns The result of validSignatures. */
  def valid: Boolean = validSignatures

} // end case class Signed

// doc
trait POWSignHelp {

  // doc
  implicit class LinearHashHelpers[T <: ProductHash](t: T) {

    // doc
    def sign2(keyPairs: Seq[KeyPair], difficulty: Int = 1): Signed[T] =
      signPairs[T](t, keyPairs, difficulty)

    // doc
    def signed(difficulty: Int = 0)(implicit keyPair: KeyPair): Signed[T] = signPairs(t, Seq(keyPair), difficulty)

    // doc
    def multiSigned(difficulty: Int = 0)(implicit keyPairs: Seq[KeyPair]): Signed[T] = signPairs(t, keyPairs, difficulty)
  }

  // doc
  def signPairs[T <: ProductHash](
                                   t: T,
                                   keyPairs: Seq[KeyPair],
                                   difficulty: Int = 0
                                 ): Signed[T] = {

    val startTime = System.currentTimeMillis()

    val signatures = t.signKeys(keyPairs.map {
      _.getPrivate
    })
    //  val nonce = t.pow(signatures, difficulty) // tmp comment
    //  val endTime = System.currentTimeMillis() // tmp comment
    Signed(
      t, startTime, keyPairs.map {
        _.getPublic.encoded
      }, signatures
    )

  }

  // doc
  def hashSign(hash: String, keyPair: KeyPair): HashSignature = {
    HashSignature(
      signHashWithKeyB64(hash, keyPair.getPrivate),
      keyPair.getPublic.encoded.b58Encoded
    )
  }

  // doc
  def hashSignBatchZeroTyped(productHash: ProductHash, keyPair: KeyPair): SignatureBatch = {
    val hash = productHash.hash
    SignatureBatch(hash, Seq(hashSign(hash, keyPair)))
  }

  // doc
  def signedObservationEdge(oe: ObservationEdge)(implicit kp: KeyPair): SignedObservationEdge = {
    SignedObservationEdge(hashSignBatchZeroTyped(oe, kp))
  }

  /** Transaction builder (for local use)
    *
    * @param src        ... Source address
    * @param dst        ... Destination address
    * @param amount     ... Quantity
    * @param keyPair    ... Signing pair matching source
    * @param normalized ... Whether quantity is normalized by NormalizationFactor (1e-8)
    * @return The resolved transaction in edge format
    */
  def createTransaction(src: String,
                        dst: String,
                        amount: Long,
                        keyPair: KeyPair,
                        normalized: Boolean = true
                       ): Transaction = {

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

} // end trait POWSignHelp

// doc
object SignHelp extends POWSignHelp
