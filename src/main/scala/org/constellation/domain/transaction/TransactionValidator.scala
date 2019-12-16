package org.constellation.domain.transaction

import cats.data.ValidatedNel
import cats.effect.Sync
import cats.implicits._
import constellation._
import org.constellation.primitives.Transaction

object TransactionValidator {
  type ValidationResult[A] = ValidatedNel[TransactionValidationError, A]

  def validateSourceSignature(tx: Transaction): ValidationResult[Transaction] = {
    val isValid = tx.signatures.exists { hs ⇒
      hs.publicKey.address == tx.src.address && hs.valid(
        tx.signaturesHash
      )
    }

    if (isValid) tx.validNel else InvalidSourceSignature(tx).invalidNel
  }

  def validateEmptyDestinationAddress(tx: Transaction): ValidationResult[Transaction] =
    if (tx.dst.address.nonEmpty) tx.validNel else EmptyDestinationAddress(tx).invalidNel

  def validateDestinationAddress(tx: Transaction): ValidationResult[Transaction] =
    if (tx.dst.address.length > 30 && tx.dst.address.startsWith("DAG")&& tx.src != tx.dst)
      tx.validNel
    else
      InvalidDestinationAddress(tx).invalidNel

  def validateAmount(tx: Transaction): ValidationResult[Transaction] =
    if (tx.isDummy) {
      if (tx.amount == 0) tx.validNel else NonZeroAmount(tx).invalidNel
    } else {
      if (tx.amount > 0) tx.validNel else NonPositiveAmount(tx).invalidNel
    }

  def validateFee(tx: Transaction): ValidationResult[Transaction] =
    tx.fee match {
      case Some(fee) if fee <= 0 => NonPositiveFee(tx).invalidNel
      case _                     => tx.validNel
    }
}

class TransactionValidator[F[_]: Sync](
  transactionService: TransactionService[F]
) {

  import TransactionValidator._

  def validateDuplicate(tx: Transaction): F[ValidationResult[Transaction]] =
    transactionService.isAccepted(tx.hash).map { isAccepted =>
      if (isAccepted)
        HashDuplicateFound(tx).invalidNel
      else
        tx.validNel
    }

  def validateTransaction(tx: Transaction): F[ValidationResult[Transaction]] =
    for {
      staticValidation <- Sync[F].delay(
        validateSourceSignature(tx)
          .product(validateEmptyDestinationAddress(tx))
          .product(validateDestinationAddress(tx))
          .product(validateAmount(tx))
          .product(validateFee(tx))
      )
      duplicateValidation <- validateDuplicate(tx)
    } yield staticValidation.product(duplicateValidation).map(_ ⇒ tx)

}

sealed trait TransactionValidationError {
  def errorMessage: String
}

case class InvalidSourceSignature(txHash: String) extends TransactionValidationError {
  def errorMessage: String = s"Transaction tx=$txHash has invalid source signature"
}

object InvalidSourceSignature {
  def apply(tx: Transaction) = new InvalidSourceSignature(tx.hash)
}

case class EmptyDestinationAddress(txHash: String) extends TransactionValidationError {
  def errorMessage: String = s"Transaction tx=$txHash has an empty destination address"
}

object EmptyDestinationAddress {
  def apply(tx: Transaction) = new EmptyDestinationAddress(tx.hash)
}

case class InvalidDestinationAddress(txHash: String, address: String) extends TransactionValidationError {
  def errorMessage: String = s"Transaction tx=$txHash has an invalid destination address=$address"
}

object InvalidDestinationAddress {
  def apply(tx: Transaction) = new InvalidDestinationAddress(tx.hash, tx.dst.hash)
}

case class NonPositiveAmount(txHash: String, amount: Long) extends TransactionValidationError {
  def errorMessage: String = s"Transaction tx=$txHash has a non-positive amount=${amount.toString}"
}

object NonPositiveAmount {
  def apply(tx: Transaction) = new NonPositiveAmount(tx.hash, tx.amount)
}

case class NonZeroAmount(txHash: String, amount: Long) extends TransactionValidationError {
  def errorMessage: String = s"Transaction tx=$txHash has a non-zero amount=${amount.toString}"
}

object NonZeroAmount {
  def apply(tx: Transaction) = new NonZeroAmount(tx.hash, tx.amount)
}

case class NonPositiveFee(txHash: String, fee: Option[Long]) extends TransactionValidationError {
  def errorMessage: String = s"Transaction tx=$txHash has a non-positive fee=${fee.toString}"
}

object NonPositiveFee {
  def apply(tx: Transaction) = new NonPositiveFee(tx.hash, tx.fee)
}

case class HashDuplicateFound(txHash: String) extends TransactionValidationError {
  def errorMessage: String = s"Transaction tx=$txHash already exists"
}

object HashDuplicateFound {
  def apply(tx: Transaction) = new HashDuplicateFound(tx.hash)
}

case class PreviousTransactionHasNotBeenAccepted(txHash: String, previousHash: String)
    extends TransactionValidationError {

  def errorMessage: String =
    s"Transaction tx=$txHash has a reference to previous tx=${previousHash} which has not been accepted yet"
}

object PreviousTransactionHasNotBeenAccepted {
  def apply(tx: Transaction) = new PreviousTransactionHasNotBeenAccepted(tx.hash, tx.lastTxRef.hash)
}
