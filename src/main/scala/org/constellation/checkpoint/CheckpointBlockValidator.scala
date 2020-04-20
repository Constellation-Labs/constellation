package org.constellation.checkpoint

import cats.data.{Ior, NonEmptyList, ValidatedNel}
import cats.effect.{IO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.transaction.TransactionValidator
import org.constellation.primitives.Schema.{CheckpointCache, ObservationEdge, SignedObservationEdge}
import org.constellation.primitives.{CheckpointBlock, Transaction}
import org.constellation.storage.{AddressService, SnapshotService}
import org.constellation.util.{HashSignature, Metrics}
import org.constellation.{ConfigUtil, DAO}

object CheckpointBlockValidator {

  private val stakingAmount = ConfigUtil.getOrElse("constellation.staking-amount", 0L)

  type ValidationResult[A] = ValidatedNel[CheckpointBlockValidation, A]
  type AddressBalance = Map[String, Long]

  import cats.implicits._

  def validateDuplicatedTransactions(
    t: Iterable[Transaction]
  ): ValidationResult[List[Transaction]] = {
    val diff = t.toList.diff(t.toSet.toList)

    if (diff.isEmpty) {
      t.toList.validNel
    } else {

      def toError(t: Transaction): ValidationResult[Transaction] =
        DuplicatedTransaction(t).invalidNel

      diff.map(toError(_).map(List(_))).combineAll
    }
  }

  def validateHashIntegrity(c: CheckpointBlock): ValidationResult[CheckpointBlock] =
    if (c.validHash) c.validNel else InvalidCheckpointHash(c).invalidNel

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

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def simpleValidation(cb: CheckpointBlock): F[ValidationResult[CheckpointBlock]] =
    for {
      validation <- validateCheckpointBlock(cb)
      _ <- if (validation.isValid) dao.metrics.incrementMetricAsync("checkpointValidationSuccess")
      else
        dao.metrics
          .incrementMetricAsync(Metrics.checkpointValidationFailure)
          .flatTap(
            _ =>
              logger.warn(
                s"Checkpoint block with baseHash: ${cb.baseHash} is invalid ${validation.leftMap(_.map(_.errorMessage))}"
              )
          )
    } yield validation

  def validateTransactionIntegrity(t: Transaction): F[ValidationResult[Transaction]] =
    transactionValidator
      .validateTransaction(t)
      .map(v => if (v.isValid) t.validNel else InvalidTransaction(t, v).invalidNel)

  def validateSourceAddressCache(t: Transaction)(implicit dao: DAO): F[ValidationResult[Transaction]] =
    addressService
      .lookup(t.src.address)
      .map(_.fold[ValidationResult[Transaction]](NoAddressCacheFound(t).invalidNel)(_ => t.validNel))

  def validateTransaction(t: Transaction): F[ValidationResult[Transaction]] = validateTransactionIntegrity(t)

  def validateTransactions(
    t: Iterable[Transaction]
  ): F[ValidationResult[List[Transaction]]] =
    t.toList.traverse(validateTransaction).map(x => x.map(_.map(List(_)))).map(_.combineAll)

  def validateDuplicatedTransactions(
    t: Iterable[Transaction]
  ): ValidationResult[List[Transaction]] = {
    val diff = t.toList.diff(t.toSet.toList)

    if (diff.isEmpty) {
      t.toList.validNel
    } else {

      def toError(t: Transaction): ValidationResult[Transaction] =
        DuplicatedTransaction(t).invalidNel

      diff.map(toError(_).map(List(_))).combineAll
    }
  }

  def validateSourceAddressBalances(
    t: Iterable[Transaction]
  ): F[ValidationResult[List[Transaction]]] = {

    def lookup(key: String): F[Long] =
      addressService
        .lookup(key)
        .map(
          _.map(_.balance)
            .getOrElse(0L)
        )

    def validateBalance(address: String, t: Iterable[Transaction]): F[ValidationResult[List[Transaction]]] =
      lookup(address).map { balance =>
        val amount = t.map(_.amount).map(BigInt(_)).sum
        val diff = balance - amount

        val isNodeAddress = dao.id.address == address
        val necessaryAmount = if (isNodeAddress) stakingAmount else 0L

        if (diff >= necessaryAmount && amount >= 0) t.toList.validNel
        else InsufficientBalance(address).invalidNel
      }

    t.groupBy(_.src.address)
      .toList
      .traverse(a => validateBalance(a._1, a._2))
      .map(_.combineAll)
  }

  def isInSnapshot(c: CheckpointBlock): F[Boolean] =
    snapshotService.acceptedCBSinceSnapshot.get.map(!_.contains(c.baseHash))

  def validateDiff(a: (String, Long)): F[Boolean] = a match {
    case (hash, diff) =>
      addressService
        .lookup(hash)
        .map(_.map(_.balanceByLatestSnapshot).getOrElse(0L) + diff >= 0)
  }

  def validateCheckpointBlockTree(
    cb: CheckpointBlock
  ): F[Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance]] = {

    val validate: F[Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance]] = for {
      parents <- checkpointParentService.getParents(cb)
      x <- parents.traverse(z => validateCheckpointBlockTree(z))
      folded = x
        .foldLeft(Map.empty[String, Long].rightIor[NonEmptyList[CheckpointBlockValidation]])(
          (result, d) => result.combine(d)
        )
        .map(getSummaryBalance(cb) |+| _)
      result = folded.flatMap(diffs => diffs.rightIor) // TODO: wkoszycki   how to make diffs.forallM(validateDiff)
    } yield result

    val z: F[Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance]] =
      isInSnapshot(cb).ifM(Sync[F].pure(Map.empty[String, Long].rightIor), validate)
    z
  }

  def singleTransactionValidation(tx: Transaction): F[ValidationResult[Transaction]] =
    for {
      transactionValidation <- validateTransactions(Iterable(tx))
      balanceValidation <- validateSourceAddressBalances(Iterable(tx))
    } yield transactionValidation.product(balanceValidation).map(_._1.map(_ => tx).head)

  def validateCheckpointBlock(cb: CheckpointBlock): F[ValidationResult[CheckpointBlock]] = {
    val preTreeResult =
      for {
        staticValidation <- Sync[F].delay(
          validateEmptySignatures(cb.signatures)
            .product(validateSignatures(cb.signatures, cb.baseHash))
        )
        hashValidation <- Sync[F].delay(validateHashIntegrity(cb))
        transactionValidation <- validateCheckpointBlockTransactions(cb)
      } yield staticValidation.product(transactionValidation).product(hashValidation)

    snapshotService.lastSnapshotHeight.get
      .map(_ == 0)
      .ifM(preTreeResult.map(_.map(_ => cb)), preTreeResult.map(_.map(_ => cb)))
  }

  private def validateCheckpointBlockTransactions(cb: CheckpointBlock): F[ValidationResult[List[Transaction]]] =
    for {
      transactionValidation <- validateTransactions(cb.transactions)
      duplicatedTransactions <- Sync[F].delay(validateDuplicatedTransactions(cb.transactions))
      balanceValidation <- validateSourceAddressBalances(cb.transactions)
    } yield
      transactionValidation
        .product(duplicatedTransactions)
        .product(balanceValidation)
        .map(_.combineAll)

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

case class InvalidTransaction(txHash: String, cause: String) extends CheckpointBlockValidation {

  def errorMessage: String = s"CheckpointBlock includes transaction=$txHash which is invalid, cause: $cause"
}

object InvalidCheckpointHash {

  def apply(c: CheckpointBlock) =
    new InvalidCheckpointHash(c.checkpoint.edge.observationEdge, c.checkpoint.edge.signedObservationEdge)
}

case class InvalidCheckpointHash(oe: ObservationEdge, soe: SignedObservationEdge) extends CheckpointBlockValidation {

  def errorMessage: String =
    s"CheckpointBlock received has incompatible hashes with ObservationEdge=$oe and SignedObservationEdge=$soe"
}

object InvalidTransaction {

  def apply(t: Transaction, v: TransactionValidator.ValidationResult[Transaction]) =
    new InvalidTransaction(t.hash, v.leftMap(_.map(_.errorMessage)).toString)
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

case class InsufficientBalance(address: String) extends CheckpointBlockValidation {

  def errorMessage: String =
    s"CheckpointBlock includes transaction from address=$address which has insufficient balance"
}
