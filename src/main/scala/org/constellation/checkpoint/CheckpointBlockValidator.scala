package org.constellation.checkpoint

import cats.data.{Ior, NonEmptyList, ValidatedNel}
import cats.effect.{IO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.domain.transaction.TransactionValidator
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.primitives.{CheckpointBlock, Transaction}
import org.constellation.storage.{AddressService, SnapshotService}
import org.constellation.util.{HashSignature, Metrics}

object CheckpointBlockValidator {

  type ValidationResult[A] = ValidatedNel[CheckpointBlockValidation, A]
  type AddressBalance = Map[String, Long]

  import cats.implicits._

  def validateSignatureIntegrity(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
    if (s.valid(baseHash)) s.validNel else InvalidSignature(s).invalidNel

  def validateSignature(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
    validateSignatureIntegrity(s, baseHash)
      .map(_ => s)

  def validateSignatures(s: Iterable[HashSignature], baseHash: String): ValidationResult[List[HashSignature]] =
    s.toList.map(validateSignature(_, baseHash).map(List(_))).combineAll

  def validateEmptySignatures(s: Iterable[HashSignature]): ValidationResult[List[HashSignature]] =
    if (s.nonEmpty) s.toList.validNel else EmptySignatures().invalidNel

  def getSummaryBalance(c: CheckpointBlock): AddressBalance = {
    val spend = c.transactions
      .groupBy(_.src.address)
      .mapValues(_.map(-_.amount).sum)

    val received = c.transactions
      .groupBy(_.dst.address)
      .mapValues(_.map(_.amount).sum)

    spend |+| received
  }

  implicit def validateTreeToValidated(
    v: Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance]
  ): ValidationResult[AddressBalance] =
    v match {
      case Ior.Right(a)   => a.validNel
      case Ior.Left(a)    => a.invalid
      case Ior.Both(a, _) => a.invalid
    }

  def detectInternalTipsConflict(
    inputTips: Seq[CheckpointCache]
  ): Option[CheckpointCache] = {

    def detect(
      tips: Seq[CheckpointCache],
      ancestryTransactions: Map[Transaction, String] = Map.empty
    ): Option[CheckpointCache] =
      tips match {
        case Nil => None
        case CheckpointCache(cb, children, height) :: _
            if cb.transactions.toSet.intersect(ancestryTransactions.keySet).nonEmpty =>
          val conflictingCBBaseHash = ancestryTransactions(
            cb.transactions.toSet.intersect(ancestryTransactions.keySet).head
          )
          Some(
            selectBlockToPreserve(
              Seq(CheckpointCache(cb, children, height)) ++ inputTips
                .filter(ccd => ccd.checkpointBlock.baseHash == conflictingCBBaseHash)
            )
          )
        case CheckpointCache(cb, _, _) :: tail =>
          detect(tail, ancestryTransactions ++ cb.transactions.map(i => (i, cb.baseHash)))
      }
    detect(inputTips)
  }

  def hasTransactionInCommon(cbLeft: CheckpointBlock, cbRight: CheckpointBlock): Boolean =
    cbLeft.transactions.intersect(cbRight.transactions).nonEmpty

  def selectBlockToPreserve(blocks: Iterable[CheckpointCache]): CheckpointCache =
    blocks.maxBy(
      cb => (cb.children, (cb.checkpointBlock.signatures.size, cb.checkpointBlock.baseHash))
    )

}

class CheckpointBlockValidator[F[_]: Sync](
  addressService: AddressService[F],
  snapshotService: SnapshotService[F],
  checkpointParentService: CheckpointParentService[F],
  transactionValidator: TransactionValidator[F],
  dao: DAO
) {

  import CheckpointBlockValidator._

  private val logger = Slf4jLogger.getLogger[F]

  def simpleValidation(cb: CheckpointBlock): F[ValidationResult[CheckpointBlock]] =
    for {
      _ <- logger.debug(s"Start Validation for CheckpointBlock : ${cb.baseHash}")
      validationResult <- validateCheckpointBlock(cb)
      _ <- logger.debug(s"Validation finish for CheckpointBlock : ${cb.baseHash}")

      _ <- updateMetrics(validationResult, cb.baseHash)
    } yield validationResult

  private def validateCheckpointBlock(cb: CheckpointBlock): F[ValidationResult[CheckpointBlock]] = {
    val preTreeResult =
      for {
        _ <- logger.debug(s"Static Validation for CheckpointBlock : ${cb.baseHash}")
        staticValidation <- Sync[F].delay(
          validateEmptySignatures(cb.signatures).product(validateSignatures(cb.signatures, cb.baseHash))
        )

        _ <- logger.debug(s"Transactions Validation for CheckpointBlock : ${cb.baseHash}")
        transactionsValidation <- validateTransactions(cb.transactions)

        _ <- logger.debug(s"DuplicatedTransactions Validation for CheckpointBlock : ${cb.baseHash}")
        duplicatedTransactionsValidation <- Sync[F].delay(validateDuplicatedTransactions(cb.transactions))

        _ <- logger.debug(s"Balance Validation Validation for CheckpointBlock : ${cb.baseHash}")
        balanceValidation <- validateSourceAddressBalances(cb.transactions)
      } yield
        staticValidation
          .product(transactionsValidation)
          .product(duplicatedTransactionsValidation)
          .product(balanceValidation)

    snapshotService.lastSnapshotHeight.get
      .map(_ == 0)
      .flatMap(_ => preTreeResult.map(_.map(_ => cb)))
  }

  private def updateMetrics(result: ValidationResult[CheckpointBlock], cbHash: String): F[Unit] =
    if (result.isValid)
      dao.metrics.incrementMetricAsync(Metrics.checkpointValidationSuccess)
    else
      dao.metrics
        .incrementMetricAsync(Metrics.checkpointValidationFailure)
        .flatTap(_ => logger.warn(s"Checkpoint block with baseHash : $cbHash is invalid : $result"))

  private def validateTransactionIntegrity(t: Transaction): F[ValidationResult[Transaction]] =
    transactionValidator
      .validateTransaction(t)
      .map(v => if (v.isValid) t.validNel else InvalidTransaction(t).invalidNel)

  private def validateTransactions(txs: Iterable[Transaction]): F[ValidationResult[List[Transaction]]] =
    txs.toList
      .traverse(validateTransactionIntegrity)
      .map(x => x.map(_.map(List(_))))
      .map(_.combineAll)

  private def validateDuplicatedTransactions(txs: Iterable[Transaction]): ValidationResult[List[Transaction]] = {
    val diff = txs.toList.diff(txs.toSet.toList)

    if (diff.isEmpty) {
      txs.toList.validNel
    } else {

      def toError(t: Transaction): ValidationResult[Transaction] =
        DuplicatedTransaction(t).invalidNel

      diff.map(toError(_).map(List(_))).combineAll
    }
  }

  private def validateSourceAddressBalances(txs: Iterable[Transaction]): F[ValidationResult[List[Transaction]]] = {

    def lookup(key: String): F[Long] =
      addressService
        .lookup(key)
        .map(
          _.map(_.balance)
            .getOrElse(0L)
        )

    def validateBalance(address: String, t: Iterable[Transaction]): F[ValidationResult[List[Transaction]]] =
      lookup(address).map { a =>
        val diff = a - t.map(_.amount).sum
        val amount = t.map(_.amount).sum

        if (diff >= 0L) t.toList.validNel else InsufficientBalance(address, amount, diff).invalidNel
      }

    txs
      .groupBy(_.src.address)
      .toList
      .traverse(a => validateBalance(a._1, a._2))
      .map(_.combineAll)
  }

  def isInSnapshot(c: CheckpointBlock): F[Boolean] =
    snapshotService.acceptedCBSinceSnapshot.get.map(!_.contains(c.baseHash))

  def getTransactionsTillSnapshot(
    cbs: List[CheckpointBlock]
  ): F[List[String]] = {

    def getParentTransactions(
      parents: Seq[CheckpointBlock],
      accu: List[String] = List.empty,
      snapshotReached: Boolean = false
    ): F[List[String]] =
      parents match {
        case Nil => Sync[F].pure(accu)
        case cb :: tail =>
          isInSnapshot(cb).flatMap { isIn =>
            if (isIn || snapshotReached) {
              getParentTransactions(
                tail,
                accu ++ cb.transactions.map(_.hash),
                snapshotReached
              )
            } else {
              checkpointParentService
                .getParents(cb)
                .flatMap(
                  parents =>
                    getParentTransactions(
                      tail ++ parents,
                      accu ++ cb.transactions.map(_.hash),
                      isIn
                    )
                )
            }

          }
      }

    cbs
      .traverse(cb => isInSnapshot(cb).map((_, cb)))
      .flatMap(
        x =>
          x.filterNot(_._1)
            .traverse(
              z =>
                checkpointParentService
                  .getParents(z._2)
                  .flatMap(parents => getParentTransactions(parents, z._2.transactions.map(_.hash).toList))
            )
            .map(_.flatten)
      )
  }

  def containsAlreadyAcceptedTx(cb: CheckpointBlock): IO[List[String]] = {
    val containsAccepted = cb.transactions.toList.map { t =>
      dao.transactionService.lookup(t.hash).map {
        case Some(tx) if tx.cbBaseHash != cb.baseHash.some => (t.hash, true)
        case _                                             => (t.hash, false)
      }
      dao.transactionService.isAccepted(t.hash).map(b => (t.hash, b))
    }.sequence[IO, (String, Boolean)]
      .map(l => l.collect { case (h, true) => h })

    containsAccepted
  }
}

sealed trait CheckpointBlockValidation {

  def errorMessage: String
}

case class EmptySignatures() extends CheckpointBlockValidation {

  def errorMessage: String = "CheckpointBlock has no signatures"
}

case class InvalidSignature(signature: String) extends CheckpointBlockValidation {

  def errorMessage: String = s"CheckpointBlock includes signature=$signature which is invalid"
}

object InvalidSignature {

  def apply(s: HashSignature) = new InvalidSignature(s.signature)
}

case class InvalidTransaction(txHash: String) extends CheckpointBlockValidation {

  def errorMessage: String = s"CheckpointBlock includes transaction=$txHash which is invalid"
}

object InvalidTransaction {

  def apply(t: Transaction) = new InvalidTransaction(t.hash)
}

case class DuplicatedTransaction(txHash: String) extends CheckpointBlockValidation {

  def errorMessage: String = s"CheckpointBlock includes duplicated transaction=$txHash"
}

object DuplicatedTransaction {

  def apply(t: Transaction) = new DuplicatedTransaction(t.hash)
}

case class NoAddressCacheFound(txHash: String, srcAddress: String) extends CheckpointBlockValidation {

  def errorMessage: String =
    s"CheckpointBlock includes transaction=$txHash which has no address cache for address=$srcAddress"
}

object NoAddressCacheFound {

  def apply(t: Transaction) = new NoAddressCacheFound(t.hash, t.src.address)
}

case class InsufficientBalance(address: String, amount: Long, diff: Long) extends CheckpointBlockValidation {

  def errorMessage: String =
    s"CheckpointBlock includes transaction from address=$address which has insufficient balance"
}

// TODO: pass also a transaction metadata

case class InternalInconsistency(cbHash: String) extends CheckpointBlockValidation {

  def errorMessage: String =
    s"CheckpointBlock=$cbHash includes transaction/s which has insufficient balance"
}

object InternalInconsistency {

  def apply(cb: CheckpointBlock) = new InternalInconsistency(cb.baseHash)
}
