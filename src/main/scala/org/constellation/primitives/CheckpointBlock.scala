package org.constellation.primitives

import java.security.KeyPair

import cats.data.{Ior, NonEmptyList, ValidatedNel}
import constellation.signedObservationEdge
import org.constellation.DAO
import org.constellation.primitives.Schema._
import org.constellation.util.HashSignature

import cats.data._
import cats.implicits._

case class CheckpointBlock(
                            transactions: Seq[Transaction],
                            checkpoint: CheckpointEdge
                          ) {

  def storeSOE()(implicit dao: DAO): Unit = {
    dao.soeService.put(soeHash, SignedObservationEdgeCache(soe, resolved = true))
  }

  def calculateHeight()(implicit dao: DAO): Option[Height] = {

    val parents = parentSOEBaseHashes.map {
      dao.checkpointService.get
    }

    val maxHeight = if (parents.exists(_.isEmpty)) {
      None
    } else {

      val parents2 = parents.map {_.get}
      val heights = parents2.map {_.height.map{_.max}}

      val nonEmptyHeights = heights.flatten
      if (nonEmptyHeights.isEmpty) None else {
        Some(nonEmptyHeights.max + 1)
      }
    }

    val minHeight = if (parents.exists(_.isEmpty)) {
      None
    } else {

      val parents2 = parents.map {_.get}
      val heights = parents2.map {_.height.map{_.min}}

      val nonEmptyHeights = heights.flatten
      if (nonEmptyHeights.isEmpty) None else {
        Some(nonEmptyHeights.min + 1)
      }
    }

    val height = maxHeight.flatMap{ max =>
      minHeight.map{
        min =>
          Height(min, max)
      }
    }

    height

  }

  def transactionsValid: Boolean = transactions.nonEmpty && transactions.forall(_.valid)

  // TODO: Return checkpoint validation status for more info rather than just a boolean
  def simpleValidation()(implicit dao: DAO): Boolean = {

    val validation = CheckpointBlockValidatorNel.validateCheckpointBlock(
      CheckpointBlock(transactions, checkpoint)
    )

    if (validation.isValid) {
      dao.metrics.incrementMetric("checkpointValidationSuccess")
    } else {
      dao.metrics.incrementMetric("checkpointValidationFailure")
    }

    // TODO: Return Validation instead of Boolean
    validation.isValid
  }

  def uniqueSignatures: Boolean = signatures.groupBy(_.id).forall(_._2.size == 1)

  def signedBy(id: Id) : Boolean = witnessIds.contains(id)

  def hashSignaturesOf(id: Id) : Seq[HashSignature] = signatures.filter(_.id == id)

  def signatureConflict(other: CheckpointBlock): Boolean = {
    signatures.exists{s =>
      other.signatures.exists{ s2 =>
        s.signature != s2.signature && s.id == s2.id
      }
    }
  }

  def witnessIds: Seq[Id] = signatures.map{_.id}

  def signatures: Seq[HashSignature] = checkpoint.edge.signedObservationEdge.signatureBatch.signatures

  def baseHash: String = checkpoint.edge.baseHash

  def validSignatures: Boolean = signatures.forall(_.valid(baseHash))

  // TODO: Optimize call, should store this value instead of recalculating every time.
  def soeHash: String = checkpoint.edge.signedObservationEdge.hash

  def store(cache: CheckpointCacheData)(implicit dao: DAO): Unit = {
    /*
          transactions.foreach { rt =>
            rt.edge.store(db, Some(TransactionCacheData(rt, inDAG = inDAG, resolved = true)))
          }
    */
    // checkpoint.edge.storeCheckpointData(db, {prevCache: CheckpointCacheData => cache.plus(prevCache)}, cache, resolved)
    dao.checkpointService.put(baseHash, cache)

  }

  def plus(keyPair: KeyPair): CheckpointBlock = {
    this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.withSignatureFrom(keyPair)))
  }

  def plus(hs: HashSignature): CheckpointBlock = {
    this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.withSignature(hs)))
  }

  def plus(other: CheckpointBlock): CheckpointBlock = {
    this.copy(checkpoint = checkpoint.plus(other.checkpoint))
  }

  def +(other: CheckpointBlock): CheckpointBlock = {
    this.copy(checkpoint = checkpoint.plus(other.checkpoint))
  }

  def parentSOE: Seq[TypedEdgeHash] = checkpoint.edge.parents
  def parentSOEHashes: Seq[String] = checkpoint.edge.parentHashes

  def parentSOEBaseHashes()(implicit dao: DAO): Seq[String] =
    parentSOEHashes.flatMap{dao.soeService.get}.map{_.signedObservationEdge.baseHash}

  def soe: SignedObservationEdge = checkpoint.edge.signedObservationEdge

}


object CheckpointBlock {

  def createCheckpointBlockSOE(
                             transactions: Seq[Transaction],
                             tips: Seq[SignedObservationEdge],
                             messages: Seq[ChannelMessage] = Seq()
                           )(implicit keyPair: KeyPair): CheckpointBlock = {
    createCheckpointBlock(transactions, tips.map{t => TypedEdgeHash(t.hash, EdgeHashType.CheckpointHash)}, messages )
  }

  def createCheckpointBlock(
                             transactions: Seq[Transaction],
                             tips: Seq[TypedEdgeHash],
                             messages: Seq[ChannelMessage] = Seq()
                           )(implicit keyPair: KeyPair): CheckpointBlock = {

    val checkpointEdgeData =
      CheckpointEdgeData(transactions.map { _.hash }.sorted, messages)

    val observationEdge = ObservationEdge(
      tips.toList,
      TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash)
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(observationEdge, soe, checkpointEdgeData)
    )

    CheckpointBlock(transactions, checkpointEdge)
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


case class NoAddressCacheFound(txHash: String) extends CheckpointBlockValidation {
  def errorMessage: String = s"CheckpointBlock includes transaction=$txHash which has no address cache"
}

object NoAddressCacheFound {
  def apply(t: Transaction) = new NoAddressCacheFound(t.hash)
}


case class InsufficientBalance(address: String) extends CheckpointBlockValidation {
  def errorMessage: String = s"CheckpointBlock includes transaction from address=$address which has insufficient balance"
}

object InsufficientBalance {
  def apply(t: Transaction) = new InsufficientBalance(t.src.address)
}


// TODO: pass also a transaction metadata
case class InternalInconsistency(cbHash: String) extends CheckpointBlockValidation {
  def errorMessage: String = s"CheckpointBlock=$cbHash includes transaction/s which has insufficient balance"
}

object InternalInconsistency {
  def apply(cb: CheckpointBlock) = new InternalInconsistency(cb.baseHash)
}

sealed trait CheckpointBlockValidatorNel {

  type ValidationResult[A] = ValidatedNel[CheckpointBlockValidation, A]

  def validateTransactionIntegrity(t: Transaction): ValidationResult[Transaction] =
    if (t.valid) t.validNel else InvalidTransaction(t).invalidNel

  def validateSourceAddressCache(t: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
    dao.addressService
      .get(t.src.address)
      .fold[ValidationResult[Transaction]](NoAddressCacheFound(t).invalidNel)(_ => t.validNel)

  def validateTransaction(t: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
    validateTransactionIntegrity(t)
      .product(validateSourceAddressCache(t))
      .map(_ => t)

  def validateTransactions(t: Iterable[Transaction])(implicit dao: DAO): ValidationResult[List[Transaction]] =
    t.toList.map(validateTransaction(_).map(List(_))).combineAll

  def validateDuplicatedTransactions(t: Iterable[Transaction]): ValidationResult[List[Transaction]] = {
    val diff = t.toList.diff(t.toSet.toList)

    if (diff.isEmpty) {
      t.toList.validNel
    } else {
      def toError(t: Transaction): ValidationResult[Transaction] = DuplicatedTransaction(t).invalidNel

      diff.map(toError(_).map(List(_))).combineAll
    }
  }

  def validateSignatureIntegrity(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
    if (s.valid(baseHash)) s.validNel else InvalidSignature(s).invalidNel

  def validateSignature(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
    validateSignatureIntegrity(s, baseHash)
      .map(_ => s)

  def validateSignatures(s: Iterable[HashSignature], baseHash: String): ValidationResult[List[HashSignature]] =
    s.toList.map(validateSignature(_, baseHash).map(List(_))).combineAll

  def validateEmptySignatures(s: Iterable[HashSignature]): ValidationResult[List[HashSignature]] =
    if (s.nonEmpty) s.toList.validNel else EmptySignatures().invalidNel

  def validateSourceAddressBalances(
                                     t: Iterable[Transaction]
                                   )(implicit dao: DAO): ValidationResult[List[Transaction]] = {
    def lookup(key: String) = dao.addressService
      .get(key)
      .map(_.balance)
      .getOrElse(0L)

    def validateBalance(address: String, t: Iterable[Transaction]): ValidationResult[List[Transaction]] = {
      val diff = lookup(address) - t.map(_.amount).sum

      if (diff >= 0L) t.toList.validNel else InsufficientBalance(address).invalidNel
    }

    t.toList
      .groupBy(_.src.address)
      .map(a => validateBalance(a._1, a._2))
      .toList
      .combineAll
  }

  type AddressBalance = Map[String, Long]

  def getParents(c: CheckpointBlock)(implicit dao: DAO): List[CheckpointBlock] =
    c.parentSOEBaseHashes
      .toList
      .map(dao.checkpointService.get)
      .map(_.flatMap(_.checkpointBlock))
      .sequence[Option, CheckpointBlock]
      .getOrElse(List())

  def isInSnapshot(c: CheckpointBlock)(implicit dao: DAO): Boolean =
    dao.threadSafeTipService
      .acceptedCBSinceSnapshot
      .contains(c.baseHash)

  def getSummaryBalance(c: CheckpointBlock)(implicit dao: DAO): AddressBalance = {
    val spend = c.transactions
      .groupBy(_.src.address)
      .mapValues(_.map(-_.amount).sum)

    val received = c.transactions
      .groupBy(_.dst.address)
      .mapValues(_.map(_.amount).sum)

    spend |+| received
  }

  /*
      def getSnapshotBalances(implicit dao: DAO): AddressBalance =
        dao.threadSafeTipService
          .getSnapshotInfo()
          .addressCacheData
          .mapValues(_.balanceByLatestSnapshot)
  */

  def validateDiff(a: (String, Long))(implicit dao: DAO): Boolean = a match {
    case (hash, diff) => dao.addressService.get(hash).map{_.balanceByLatestSnapshot}.getOrElse(0L) + diff >= 0
  }

  def validateCheckpointBlockTree(cb: CheckpointBlock)(implicit dao: DAO): Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance] =
    if (isInSnapshot(cb)) Map.empty[String, Long].rightIor
    else
      getParents(cb)
        .map(validateCheckpointBlockTree)
        .foldLeft(Map.empty[String, Long].rightIor[NonEmptyList[CheckpointBlockValidation]])((result, d) =>
          result.combine(d))
        .map(getSummaryBalance(cb) |+| _)
        .flatMap(diffs =>
          if (diffs.forall(validateDiff))
            diffs.rightIor
          else
            Ior.both(NonEmptyList.of(InternalInconsistency(cb)), diffs))

  implicit def validateTreeToValidated(v: Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance]): ValidationResult[AddressBalance] =
    v match {
      case Ior.Right(a) => a.validNel
      case Ior.Left(a) => a.invalid
      case Ior.Both(a, _) => a.invalid
    }

  def validateCheckpointBlock(
                               cb: CheckpointBlock
                             )(implicit dao: DAO): ValidationResult[CheckpointBlock] = {
    val preTreeResult =
      validateEmptySignatures(cb.signatures)
        .product(validateSignatures(cb.signatures, cb.baseHash))
        .product(validateTransactions(cb.transactions))
        .product(validateDuplicatedTransactions(cb.transactions))
        .product(validateSourceAddressBalances(cb.transactions))

    val postTreeIgnoreEmptySnapshot = if (dao.threadSafeTipService.lastSnapshotHeight == 0) preTreeResult
    else preTreeResult.product(validateCheckpointBlockTree(cb))

    postTreeIgnoreEmptySnapshot.map(_ => cb)
  }
}

object CheckpointBlockValidatorNel extends CheckpointBlockValidatorNel
