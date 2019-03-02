package org.constellation.primitives

import java.security.KeyPair

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits._
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.{Address, TransactionCacheData, TransactionEdgeData}
import org.constellation.util.HashSignature

case class Transaction(edge: Edge[TransactionEdgeData]) {

  def store(cache: TransactionCacheData)(implicit dao: DAO): Unit = {
    dao.acceptedTransactionService.put(this.hash, cache)
  }

  def ledgerApply()(implicit dao: DAO): Unit = {
    dao.addressService.transfer(src, dst, amount).unsafeRunSync()
  }

  def ledgerApplySnapshot()(implicit dao: DAO): Unit = {
    dao.addressService.transferSnapshot(src, dst, amount).unsafeRunSync()
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

  type ValidationResult[A] = ValidatedNel[TransactionValidation, A]

  def validateSourceSignature(): ValidationResult[Transaction] =
    validSrcSignature.toOption(this.validNel).getOrElse(InvalidSourceSignature().invalidNel)

  def validateHashDuplicateSnapshot()(implicit dao: DAO): ValidationResult[Transaction] = {
    dao.transactionHashStore.contains(hash).map { c =>
      if (c) this.validNel else HashDuplicateFoundInSnapshot().invalidNel
    }.get
  }

}

sealed trait TransactionValidation {
  def errorMessage: String
}

case class HashDuplicateFoundInSnapshot() extends TransactionValidation {
  def errorMessage: String = "Transaction hash already exists in old data"
}

case class HashDuplicateFoundInRecent() extends TransactionValidation {
  def errorMessage: String = "Transaction hash already exists in recent data"
}

case class InvalidSourceSignature() extends TransactionValidation {
  def errorMessage: String = s"Transaction has invalid source signature"
}
