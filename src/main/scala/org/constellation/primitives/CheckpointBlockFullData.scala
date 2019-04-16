package org.constellation.primitives

import java.security.KeyPair

import cats.data.{Ior, NonEmptyList, ValidatedNel}
import cats.implicits._
import constellation.signedObservationEdge
import org.constellation.DAO
import org.constellation.primitives.Schema._
import org.constellation.primitives.storage.{CheckpointService, StorageService}
import org.constellation.util.{HashSignature, MerkleTree}

object CheckpointBlockData {

  def apply(data: CheckpointBlockFullData)(implicit dao: DAO): CheckpointBlockData = {
      CheckpointBlockData(
        add(data.transactions.map(_.hash), dao.transactionService.merklePool).get,
        data.checkpoint,
        add(data.messages.map(_.signedMessageData.hash),
            dao.messageService.merklePool),
        add(data.notifications.map(_.hash),
          dao.notificationService.merklePool)
        ,
      )
    }

  private def add(data: Seq[String], ss: StorageService[Seq[String]]): Option[String] = {
      data match {
        case Seq() => None
        case _ =>
          val rootHash = MerkleTree(data).rootHash
          ss.putSync(rootHash, data)
          Some(rootHash)
      }
    }
}

abstract class CheckpointEdgeLike(val checkpoint: CheckpointEdge) {
    def baseHash: String = checkpoint.edge.baseHash

    def parentSOEHashes: Seq[String] = checkpoint.edge.parentHashes

    def parentSOEBaseHashes()(implicit dao: DAO): Seq[String] =
      checkpoint.edge.parentHashes.flatMap { dao.soeService.getSync }.map {
        _.signedObservationEdge.baseHash
      }

    def storeSOE()(implicit dao: DAO): Unit = {
      dao.soeService.putSync(soeHash, SignedObservationEdgeCache(soe, resolved = true))
    }
    def soe: SignedObservationEdge = checkpoint.edge.signedObservationEdge

    def soeHash: String = checkpoint.edge.signedObservationEdge.hash

    def signatures: Seq[HashSignature] =
      checkpoint.edge.signedObservationEdge.signatureBatch.signatures
  }

case class CheckpointBlockData(
    transactionsMerkleRoot: String,
    checkpointEdge: CheckpointEdge,
    messagesMerkleRoot: Option[String],
    notificationsMerkleRoot: Option[String]
  ) extends CheckpointEdgeLike(checkpointEdge)

case class CheckpointBlockFullData(
  transactions: Seq[Transaction],
  checkpoint: CheckpointEdge,
  messages: Seq[ChannelMessage] = Seq(),
  notifications: Seq[PeerNotification] = Seq()
) {

  def storeSOE()(implicit dao: DAO): Unit = {
    dao.soeService.putSync(soeHash, SignedObservationEdgeCache(soe, resolved = true))
  }

  def calculateHeight()(implicit dao: DAO): Option[Height] = {

    val parents = parentSOEBaseHashes.map {
      dao.checkpointService.get
    }

    val maxHeight = if (parents.exists(_.isEmpty)) {
      None
    } else {

      val parents2 = parents.map { _.get }

      val nonEmptyHeights = parents2.map { _.height.max }
      if (nonEmptyHeights.isEmpty) None
      else {
        Some(nonEmptyHeights.max + 1)
      }
    }

    val minHeight = if (parents.exists(_.isEmpty)) {
      None
    } else {

      val parents2 = parents.map { _.get }

      val nonEmptyHeights = parents2.map { _.height.max }
      if (nonEmptyHeights.isEmpty) None
      else {
        Some(nonEmptyHeights.min + 1)
      }
    }

    val height = maxHeight.flatMap { max =>
      minHeight.map { min =>
        Height(min, max)
      }
    }

    height

  }

  def transactionsValid(implicit dao: DAO): Boolean = transactions.nonEmpty && transactions.forall(_.valid)

  // TODO: Return checkpoint validation status for more info rather than just a boolean

  def simpleValidation()(implicit dao: DAO): Boolean = {

    val validation = CheckpointBlockValidatorNel.validateCheckpointBlock(transactions, CheckpointBlockData(this))

    if (validation.isValid) {
      dao.metrics.incrementMetric("checkpointValidationSuccess")
    } else {
      dao.metrics.incrementMetric("checkpointValidationFailure")
    }

    // TODO: Return Validation instead of Boolean
    validation.isValid
  }

  def uniqueSignatures: Boolean = signatures.groupBy(_.id).forall(_._2.size == 1)

  def signedBy(id: Id): Boolean = witnessIds.contains(id)

  def hashSignaturesOf(id: Id): Seq[HashSignature] = signatures.filter(_.id == id)

  def signatureConflict(other: CheckpointBlockFullData): Boolean = {
    signatures.exists { s =>
      other.signatures.exists { s2 =>
        s.signature != s2.signature && s.id == s2.id
      }
    }
  }

  def witnessIds: Seq[Id] = signatures.map { _.id }

  def signatures: Seq[HashSignature] =
    checkpoint.edge.signedObservationEdge.signatureBatch.signatures

  def baseHash: String = checkpoint.edge.baseHash

  def validSignatures: Boolean = signatures.forall(_.valid(baseHash))

  // TODO: Optimize call, should store this value instead of recalculating every time.

  def soeHash: String = checkpoint.edge.signedObservationEdge.hash

  def store(cache: CheckpointCacheFullData)(implicit dao: DAO): Unit = {
    cache.checkpointBlock.foreach { cb =>
      val data = CheckpointCacheData(CheckpointBlockData(cb), cache.children, cache.height.get)
      dao.checkpointService.memPool.put(baseHash, data).unsafeRunSync()
      dao.recentBlockTracker.put(data)
    }
  }

  def plus(keyPair: KeyPair): CheckpointBlockFullData = {
    this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.withSignatureFrom(keyPair)))
  }

  def plus(hs: HashSignature): CheckpointBlockFullData = {
    this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.withSignature(hs)))
  }

  def plus(other: CheckpointBlockFullData): CheckpointBlockFullData = {
    this.copy(checkpoint = checkpoint.plus(other.checkpoint))
  }

  def +(other: CheckpointBlockFullData): CheckpointBlockFullData = {
    this.copy(checkpoint = checkpoint.plus(other.checkpoint))
  }

  def parentSOE: Seq[TypedEdgeHash] = checkpoint.edge.parents

  def parentSOEHashes: Seq[String] = checkpoint.edge.parentHashes

  def parentSOEBaseHashes()(implicit dao: DAO): Seq[String] =
    parentSOEHashes.flatMap { dao.soeService.getSync }.map { _.signedObservationEdge.baseHash }

  def soe: SignedObservationEdge = checkpoint.edge.signedObservationEdge

}

object CheckpointBlockFullData {

  def createCheckpointBlockSOE(
    transactions: Seq[Transaction],
    tips: Seq[SignedObservationEdge],
    messages: Seq[ChannelMessage] = Seq.empty,
    peers: Seq[PeerNotification] = Seq.empty
  )(implicit keyPair: KeyPair): CheckpointBlockFullData = {
    createCheckpointBlock(transactions, tips.map { t =>
      TypedEdgeHash(t.hash, EdgeHashType.CheckpointHash)
    }, messages, peers)
  }

  def createCheckpointBlock(
    transactions: Seq[Transaction],
    tips: Seq[TypedEdgeHash],
    messages: Seq[ChannelMessage] = Seq.empty,
    peers: Seq[PeerNotification] = Seq.empty
  )(implicit keyPair: KeyPair): CheckpointBlockFullData = {

    val checkpointEdgeData =
      CheckpointEdgeData(transactions.map { _.hash }.sorted, messages.map {
        _.signedMessageData.hash
      })

    val observationEdge = ObservationEdge(
      tips.toList,
      TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash)
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(observationEdge, soe, checkpointEdgeData)
    )

    CheckpointBlockFullData(transactions, checkpointEdge, messages, peers)
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

  def errorMessage: String =
    s"CheckpointBlock includes transaction=$txHash which has no address cache"
}

object NoAddressCacheFound {

  def apply(t: Transaction) = new NoAddressCacheFound(t.hash)
}

case class InsufficientBalance(address: String) extends CheckpointBlockValidation {

  def errorMessage: String =
    s"CheckpointBlock includes transaction from address=$address which has insufficient balance"
}

object InsufficientBalance {

  def apply(t: Transaction) = new InsufficientBalance(t.src.address)
}

// TODO: pass also a transaction metadata

case class InternalInconsistency(cbHash: String) extends CheckpointBlockValidation {

  def errorMessage: String =
    s"CheckpointBlock=$cbHash includes transaction/s which has insufficient balance"
}

object InternalInconsistency {

  def apply(cb: CheckpointBlockData) = new InternalInconsistency(cb.baseHash)
}

sealed trait CheckpointBlockValidatorNel {

  type ValidationResult[A] = ValidatedNel[CheckpointBlockValidation, A]

  def validateTransactionIntegrity(t: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
    if (t.valid) t.validNel else InvalidTransaction(t).invalidNel

  def validateSourceAddressCache(t: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
    dao.addressService
      .getSync(t.src.address)
      .fold[ValidationResult[Transaction]](NoAddressCacheFound(t).invalidNel)(_ => t.validNel)

  def validateTransaction(t: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
    validateTransactionIntegrity(t)
      .product(validateSourceAddressCache(t))
      .map(_ => t)

  def validateTransactions(
    t: Iterable[Transaction]
  )(implicit dao: DAO): ValidationResult[List[Transaction]] =
    t.toList.map(validateTransaction(_).map(List(_))).combineAll

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

  def validateSignatureIntegrity(s: HashSignature,
                                 baseHash: String): ValidationResult[HashSignature] =
    if (s.valid(baseHash)) s.validNel else InvalidSignature(s).invalidNel

  def validateSignature(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
    validateSignatureIntegrity(s, baseHash)
      .map(_ => s)

  def validateSignatures(s: Iterable[HashSignature],
                         baseHash: String): ValidationResult[List[HashSignature]] =
    s.toList.map(validateSignature(_, baseHash).map(List(_))).combineAll

  def validateEmptySignatures(s: Iterable[HashSignature]): ValidationResult[List[HashSignature]] =
    if (s.nonEmpty) s.toList.validNel else EmptySignatures().invalidNel

  def validateSourceAddressBalances(
    t: Iterable[Transaction]
  )(implicit dao: DAO): ValidationResult[List[Transaction]] = {

    def lookup(key: String) =
      dao.addressService
        .getSync(key)
        .map(_.balance)
        .getOrElse(0L)

    def validateBalance(address: String,
                        t: Iterable[Transaction]): ValidationResult[List[Transaction]] = {
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

  def getParents(c: CheckpointBlockData)(implicit dao: DAO): List[(Seq[Transaction],CheckpointBlockData)] =
    c.parentSOEBaseHashes.toList
      .map(dao.checkpointService.get)
      .map(_.map(c => (CheckpointService.fetchTransactions(c.cb.transactionsMerkleRoot),c.cb)))
      .sequence[Option, (Seq[Transaction],CheckpointBlockData)]
      .getOrElse(List())

  def isInSnapshot(c: CheckpointBlockData)(implicit dao: DAO): Boolean =
    dao.threadSafeSnapshotService.acceptedCBSinceSnapshot
      .contains(c.baseHash)

  def getSummaryBalance(c: CheckpointBlockData)(implicit dao: DAO): AddressBalance = {
    val transactions = CheckpointService.fetchTransactions(c.transactionsMerkleRoot)
    val spend = transactions
      .groupBy(_.src.address)
      .mapValues(_.map(-_.amount).sum)

    val received = transactions
      .groupBy(_.dst.address)
      .mapValues(_.map(_.amount).sum)

    spend |+| received
  }

  def validateDiff(a: (String, Long))(implicit dao: DAO): Boolean = a match {
    case (hash, diff) =>
      dao.addressService.getSync(hash).map { _.balanceByLatestSnapshot }.getOrElse(0L) + diff >= 0
  }

  def validateCheckpointBlockTree(transactions: Seq[Transaction],
    cb: CheckpointBlockData
  )(implicit dao: DAO): Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance] =
    if (isInSnapshot(cb)) Map.empty[String, Long].rightIor
    else
      getParents(cb)
        .map(p => validateCheckpointBlockTree(p._1, p._2))
        .foldLeft(Map.empty[String, Long].rightIor[NonEmptyList[CheckpointBlockValidation]])(
          (result, d) => result.combine(d)
        )
        .map(getSummaryBalance(cb) |+| _)
        .flatMap(
          diffs =>
            if (diffs.forall(validateDiff))
              diffs.rightIor
            else
              Ior.both(NonEmptyList.of(InternalInconsistency(cb)), diffs)
        )

  implicit def validateTreeToValidated(
    v: Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance]
  ): ValidationResult[AddressBalance] =
    v match {
      case Ior.Right(a)   => a.validNel
      case Ior.Left(a)    => a.invalid
      case Ior.Both(a, _) => a.invalid
    }

  def validateCheckpointBlock(transactions: Seq[Transaction],
    cb: CheckpointBlockData
  )(implicit dao: DAO): ValidationResult[CheckpointBlockData] = {
    val preTreeResult =
      validateEmptySignatures(cb.signatures)
        .product(validateSignatures(cb.signatures, cb.baseHash))
        .product(validateTransactions(transactions))
        .product(validateDuplicatedTransactions(transactions))
        .product(validateSourceAddressBalances(transactions))

    val postTreeIgnoreEmptySnapshot =
      if (dao.threadSafeSnapshotService.lastSnapshotHeight == 0) preTreeResult
      else preTreeResult.product(validateCheckpointBlockTree(transactions, cb))

    postTreeIgnoreEmptySnapshot.map(_ => cb)
  }
}

object CheckpointBlockValidatorNel extends CheckpointBlockValidatorNel
