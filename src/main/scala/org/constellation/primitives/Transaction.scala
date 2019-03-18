package org.constellation.primitives

import java.security.KeyPair
import java.time.Instant

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits._
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.{Address, Id, TransactionEdgeData}
import org.constellation.util.HashSignature

sealed trait TransactionValidationStatus {
  val validationTime: Long
}
case class TransactionConfirmed(validationTime: Long) extends TransactionValidationStatus
case class TransactionRejected(validationTime: Long, reason: TransactionValidationError) extends TransactionValidationStatus

case class TransactionCacheData(
  transaction: Transaction,
  valid: Boolean = true,
  inMemPool: Boolean = false,
  inDAG: Boolean = false,
  inDAGByAncestor: Map[String, Boolean] = Map(),
  resolved: Boolean = true,
  cbBaseHash: Option[String] = None,
  cbForkBaseHashes: Set[String] = Set(),
  signatureForks: Set[Transaction] = Set(),
  knownPeers: Set[Id] = Set(),
  rxTime: Long = System.currentTimeMillis()
) {

  def plus(previous: TransactionCacheData): TransactionCacheData =
    this.copy(
      inDAGByAncestor = inDAGByAncestor ++ previous.inDAGByAncestor
        .filterKeys(k => !inDAGByAncestor.contains(k)),
      cbForkBaseHashes = (cbForkBaseHashes ++ previous.cbForkBaseHashes) -- cbBaseHash.map { s =>
        Set(s)
      }.getOrElse(Set()),
      signatureForks = (signatureForks ++ previous.signatureForks) - transaction,
      rxTime = previous.rxTime
    )
}

case class Transaction(edge: Edge[TransactionEdgeData]) {

  def store(cache: TransactionCacheData)(implicit dao: DAO): Unit = {
    dao.acceptedTransactionService.putSync(this.hash, cache)
  }

  // Unsafe

  def src: Address = Address(edge.parents.head.hash)

  def dst: Address = Address(edge.parents.last.hash)

  def signatures: Seq[HashSignature] = edge.signedObservationEdge.signatureBatch.signatures

  // TODO: Add proper exception on empty option

  def amount: Long = edge.data.amount

  def baseHash: String = edge.signedObservationEdge.baseHash

  def hash: String = edge.signedObservationEdge.hash

  def withSignatureFrom(keyPair: KeyPair): Transaction = this.copy(
    edge = edge.withSignatureFrom(keyPair)
  )

  def valid: Boolean =
    validSrcSignature &&
      dst.address.nonEmpty &&
      dst.address.length > 30 &&
      dst.address.startsWith("DAG") &&
      amount > 0

  def validSrcSignature: Boolean = {
    edge.signedObservationEdge.signatureBatch.signatures.exists { hs =>
      hs.publicKey.address == src.address && hs.valid(
        edge.signedObservationEdge.signatureBatch.hash
      )
    }
  }

  type ValidationResult[A] = ValidatedNel[TransactionValidationError, A]

  def validateSourceSignature(): ValidationResult[Transaction] =
    validSrcSignature.toOption(this.validNel).getOrElse(InvalidSourceSignature().invalidNel)

  def validateHashDuplicateSnapshot()(implicit dao: DAO): ValidationResult[Transaction] = {
    dao.transactionHashStore.contains(hash).map { c =>
      if (c) this.validNel else HashDuplicateFoundInSnapshot().invalidNel
    }.get
  }

}

sealed trait TransactionValidationError {
  def errorMessage: String
}

case class HashDuplicateFoundInSnapshot() extends TransactionValidationError {
  def errorMessage: String = "Transaction hash already exists in old data"
}

case class HashDuplicateFoundInRecent() extends TransactionValidationError {
  def errorMessage: String = "Transaction hash already exists in recent data"
}

case class InvalidSourceSignature() extends TransactionValidationError {
  def errorMessage: String = s"Transaction has invalid source signature"
}

case class TransactionSerialized(
  hash: String,
  sender: String,
  receiver: String,
  amount: Long,
  signers: Set[String],
  time: Long
) {}

object TransactionSerialized {

  def apply(tx: Transaction): TransactionSerialized =
    new TransactionSerialized(
      tx.hash,
      tx.src.address,
      tx.dst.address,
      tx.amount,
      tx.signatures.map(_.address).toSet,
      Instant.now.getEpochSecond
    )
}
