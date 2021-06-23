package org.constellation.checkpoint

import cats.data.{Ior, NonEmptyList, ValidatedNel}
import cats.effect.Sync
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.constellation.domain.transaction.{TransactionChainService, TransactionService, TransactionValidator}
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache}
import org.constellation.schema.edge.{ObservationEdge, SignedObservationEdge}
import org.constellation.schema.signature.HashSignature
import org.constellation.schema.transaction.{LastTransactionRef, Transaction}
import org.constellation.storage.{AddressService, SnapshotService}
import org.constellation.util.Metrics
import org.constellation.ConfigUtil
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.snapshot.SnapshotStorageAlgebra
import org.constellation.schema.Id

object CheckpointBlockValidator {

  type ValidationResult[A] = ValidatedNel[CheckpointBlockValidation, A]
  type AddressBalance = Map[String, Long]
  private val stakingAmount = ConfigUtil.getOrElse("constellation.staking-amount", 0L)

  import cats.syntax.all._

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

  def validateSignatures(s: Iterable[HashSignature], baseHash: String): ValidationResult[List[HashSignature]] =
    s.toList.map(validateSignature(_, baseHash).map(List(_))).combineAll

  def validateSignature(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
    validateSignatureIntegrity(s, baseHash)
      .map(_ => s)

  def validateSignatureIntegrity(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
    if (s.valid(baseHash)) s.validNel else InvalidSignature(s).invalidNel

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
                .filter(ccd => ccd.checkpointBlock.soeHash == conflictingCBBaseHash)
            )
          )
        case CheckpointCache(cb, _, _) :: tail =>
          detect(tail, ancestryTransactions ++ cb.transactions.map(i => (i, cb.soeHash)))
      }
    detect(inputTips)
  }

  def selectBlockToPreserve(blocks: Iterable[CheckpointCache]): CheckpointCache =
    blocks.maxBy(
      cb => (cb.children, (cb.checkpointBlock.signatures.size, cb.checkpointBlock.soeHash))
    )

  def hasTransactionInCommon(cbLeft: CheckpointBlock, cbRight: CheckpointBlock): Boolean =
    cbLeft.transactions.intersect(cbRight.transactions).nonEmpty

}

class CheckpointBlockValidator[F[_]: Sync](
  addressService: AddressService[F],
  snapshotStorage: SnapshotStorageAlgebra[F],
  checkpointStorage: CheckpointStorageAlgebra[F],
  transactionValidator: TransactionValidator[F],
  txChainService: TransactionChainService[F],
  metrics: Metrics,
  id: Id,
  transactionService: TransactionService[F]
) {

  import CheckpointBlockValidator._

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def simpleValidation(cb: CheckpointBlock): F[ValidationResult[CheckpointBlock]] =
    for {
      validation <- validateCheckpointBlock(cb)
      _ <- if (validation.isValid) metrics.incrementMetricAsync("checkpointValidationSuccess")
      else
        metrics
          .incrementMetricAsync(Metrics.checkpointValidationFailure)
          .flatTap(
            _ =>
              logger.warn(
                s"Checkpoint block with soeHash: ${cb.soeHash} is invalid ${validation.leftMap(_.map(_.errorMessage))}"
              )
          )
    } yield validation

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

    snapshotStorage.getLastSnapshotHeight
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

  def validateSourceAddressCache(t: Transaction): F[ValidationResult[Transaction]] =
    addressService
      .lookup(t.src.address)
      .map(_.fold[ValidationResult[Transaction]](NoAddressCacheFound(t).invalidNel)(_ => t.validNel))

  def validateLastTxRefChain(txs: Iterable[Transaction]): F[ValidationResult[List[Transaction]]] =
    txs
      .groupBy(_.src.address)
      .mapValues(_.toList.sortBy(_.ordinal))
      .toList
      .pure[F]
      .flatMap { t =>
        t.traverse {
          case (hash, txs) =>
            txChainService
              .getLastAcceptedTransactionRef(hash)
              .map { latr =>
                txs.foldLeft(latr.some) {
                  case (maybePrev, tx) =>
                    for {
                      prev <- maybePrev
                      txPrev <- tx.lastTxRef.some if prev == txPrev
                    } yield LastTransactionRef(tx.hash, tx.ordinal)
                }
              }
        }
      }
      .map(chain => if (chain.forall(_.isDefined)) txs.toList.validNel else TransactionChainIncorrect().invalidNel)

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
      parents <- checkpointStorage.getParents(cb.soeHash).map(_.sequence.flatten.map(_.checkpointBlock))
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

  def validateTransactions(
    t: Iterable[Transaction]
  ): F[ValidationResult[List[Transaction]]] =
    t.toList.traverse(validateTransaction).map(x => x.map(_.map(List(_)))).map(_.combineAll)

  def validateTransaction(t: Transaction): F[ValidationResult[Transaction]] = validateTransactionIntegrity(t)

  def validateTransactionIntegrity(t: Transaction): F[ValidationResult[Transaction]] =
    transactionValidator
      .validateTransaction(t)
      .map(v => if (v.isValid) t.validNel else InvalidTransaction(t, v).invalidNel)

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
        val amount = t.map(t => t.amount + t.feeValue).map(BigInt(_)).sum
        val diff = balance - amount

        val isNodeAddress = id.address == address
        val necessaryAmount = if (isNodeAddress) stakingAmount else 0L

        if (diff >= necessaryAmount && amount >= 0) t.toList.validNel
        else InsufficientBalance(address).invalidNel
      }

    t.groupBy(_.src.address)
      .toList
      .traverse(a => validateBalance(a._1, a._2))
      .map(_.combineAll)
  }

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
              checkpointStorage
                .getParents(cb.soeHash)
                .map(_.sequence.flatten.map(_.checkpointBlock))
                .flatMap { parents =>
                  getParentTransactions(
                    tail ++ parents,
                    accu ++ cb.transactions.map(_.hash),
                    isIn
                  )
                }
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
                checkpointStorage
                  .getParents(z._2.soeHash)
                  .map(_.sequence.flatten.map(_.checkpointBlock))
                  .flatMap(parents => getParentTransactions(parents, z._2.transactions.map(_.hash).toList))
            )
            .map(_.flatten)
      )
  }

  def isInSnapshot(c: CheckpointBlock): F[Boolean] =
    checkpointStorage.isInSnapshot(c.soeHash)

  def containsAlreadyAcceptedTx(cb: CheckpointBlock): F[List[String]] = {
    val containsAccepted = cb.transactions.toList.map { t =>
      transactionService.lookup(t.hash).map {
        case Some(tx) if tx.cbBaseHash != cb.soeHash.some => (t.hash, true)
        case _                                            => (t.hash, false)
      }
      transactionService.isAccepted(t.hash).map(b => (t.hash, b))
    }.sequence[F, (String, Boolean)]
      .map(l => l.collect { case (h, true) => h })

    containsAccepted
  }

}

sealed trait CheckpointBlockValidation {

  def errorMessage: String
}

object CheckpointBlockValidation {

  implicit val encodeCheckpointBlockValidation: Encoder[CheckpointBlockValidation] = Encoder.instance {
    case a @ EmptySignatures()           => a.asJson
    case a @ InvalidSignature(_)         => a.asJson
    case a @ InvalidTransaction(_, _)    => a.asJson
    case a @ InvalidCheckpointHash(_, _) => a.asJson
    case a @ DuplicatedTransaction(_)    => a.asJson
    case a @ NoAddressCacheFound(_, _)   => a.asJson
    case a @ InsufficientBalance(_)      => a.asJson
    case a @ TransactionChainIncorrect() => a.asJson
  }

  implicit val decodeCheckpointBlockValidation: Decoder[CheckpointBlockValidation] =
    List[Decoder[CheckpointBlockValidation]](
      Decoder[EmptySignatures].widen,
      Decoder[InvalidSignature].widen,
      Decoder[InvalidTransaction].widen,
      Decoder[InvalidCheckpointHash].widen,
      Decoder[DuplicatedTransaction].widen,
      Decoder[NoAddressCacheFound].widen,
      Decoder[InsufficientBalance].widen,
      Decoder[TransactionChainIncorrect].widen
    ).reduceLeft(_.or(_))

}

case class EmptySignatures() extends CheckpointBlockValidation {

  def errorMessage: String = "CheckpointBlock has no signatures"
}

object EmptySignatures {
  implicit val emptySignaturesEncoder: Encoder[EmptySignatures] = deriveEncoder
  implicit val emptySignaturesDecoder: Decoder[EmptySignatures] = deriveDecoder
}

case class InvalidSignature(signature: String) extends CheckpointBlockValidation {

  def errorMessage: String = s"CheckpointBlock includes signature=$signature which is invalid"
}

object InvalidSignature {
  def apply(s: HashSignature) = new InvalidSignature(s.signature)

  implicit val invalidSignatureEncoder: Encoder[InvalidSignature] = deriveEncoder
  implicit val invalidSignatureDecoder: Decoder[InvalidSignature] = deriveDecoder
}

case class InvalidTransaction(txHash: String, cause: String) extends CheckpointBlockValidation {

  def errorMessage: String = s"CheckpointBlock includes transaction=$txHash which is invalid, cause: $cause"
}

object InvalidTransaction {

  def apply(t: Transaction, v: TransactionValidator.ValidationResult[Transaction]) =
    new InvalidTransaction(t.hash, v.leftMap(_.map(_.errorMessage)).toString)

  implicit val invalidTransactionEncoder: Encoder[InvalidTransaction] = deriveEncoder
  implicit val invalidTransactionDecoder: Decoder[InvalidTransaction] = deriveDecoder
}

case class InvalidCheckpointHash(oe: ObservationEdge, soe: SignedObservationEdge) extends CheckpointBlockValidation {

  def errorMessage: String =
    s"CheckpointBlock received has incompatible hashes with ObservationEdge=$oe and SignedObservationEdge=$soe"
}

case class TransactionChainIncorrect() extends CheckpointBlockValidation {
  override def errorMessage: String = s"Transaction chain incorrect"
}

object InvalidCheckpointHash {

  def apply(c: CheckpointBlock) =
    new InvalidCheckpointHash(c.checkpoint.edge.observationEdge, c.checkpoint.edge.signedObservationEdge)

  implicit val invalidCheckpointHashEncoder: Encoder[InvalidCheckpointHash] = deriveEncoder
  implicit val invalidCheckpointHashDecoder: Decoder[InvalidCheckpointHash] = deriveDecoder
}

case class DuplicatedTransaction(txHash: String) extends CheckpointBlockValidation {

  def errorMessage: String = s"CheckpointBlock includes duplicated transaction=$txHash"
}

object DuplicatedTransaction {

  def apply(t: Transaction) = new DuplicatedTransaction(t.hash)

  implicit val duplicatedTransactionEncoder: Encoder[DuplicatedTransaction] = deriveEncoder
  implicit val duplicatedTransactionDecoder: Decoder[DuplicatedTransaction] = deriveDecoder
}

object TransactionChainIncorrect {
  implicit val transactionsChainIncorrectEncoder: Encoder[TransactionChainIncorrect] = deriveEncoder
  implicit val transactionsChainIncorrectDecoder: Decoder[TransactionChainIncorrect] = deriveDecoder
}

case class NoAddressCacheFound(txHash: String, srcAddress: String) extends CheckpointBlockValidation {

  def errorMessage: String =
    s"CheckpointBlock includes transaction=$txHash which has no address cache for address=$srcAddress"
}

object NoAddressCacheFound {

  def apply(t: Transaction) = new NoAddressCacheFound(t.hash, t.src.address)

  implicit val noAddressCacheFoundEncoder: Encoder[NoAddressCacheFound] = deriveEncoder
  implicit val noAddressCacheFoundDecoder: Decoder[NoAddressCacheFound] = deriveDecoder
}

case class InsufficientBalance(address: String) extends CheckpointBlockValidation {

  def errorMessage: String =
    s"CheckpointBlock includes transaction from address=$address which has insufficient balance"
}

object InsufficientBalance {
  implicit val insufficientBalanceEncoder: Encoder[InsufficientBalance] = deriveEncoder
  implicit val insufficientBalanceDecoder: Decoder[InsufficientBalance] = deriveDecoder
}
