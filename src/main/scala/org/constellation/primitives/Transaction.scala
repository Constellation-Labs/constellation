package org.constellation.primitives

import java.security.KeyPair
import java.time.Instant

import cats.data.ValidatedNel
import cats.implicits._
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.{Address, Id, TransactionEdgeData}
import org.constellation.util.HashSignature

case class TransactionCacheData(
  transaction: Transaction,
  inDAGByAncestor: Map[String, Boolean] = Map(),
  cbBaseHash: Option[String] = None,
  cbForkBaseHashes: Set[String] = Set(),
  signatureForks: Set[Transaction] = Set(),
  knownPeers: Set[Id] = Set(),
  rxTime: Long = System.currentTimeMillis(),
  path: Set[Id] = Set()
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

object TransactionCacheData {
  def apply(tx: Transaction): TransactionCacheData = TransactionCacheData(transaction = tx)
}

case class Transaction(edge: Edge[TransactionEdgeData]) {

  def valid(implicit dao: DAO): Boolean =
    TransactionValidatorNel.validateTransaction(this).isValid

  // Unsafe

  def src: Address = Address(edge.parents.head.hash)

  def dst: Address = Address(edge.parents.last.hash)

  def signatures: Seq[HashSignature] = edge.signedObservationEdge.signatureBatch.signatures

  // TODO: Add proper exception on empty option

  def amount: Long = edge.data.amount

  def fee: Option[Long] = edge.data.fee

  def baseHash: String = edge.signedObservationEdge.baseHash

  def hash: String = edge.signedObservationEdge.hash

  def signaturesHash: String = edge.signedObservationEdge.signatureBatch.hash

  def withSignatureFrom(keyPair: KeyPair): Transaction = this.copy(
    edge = edge.withSignatureFrom(keyPair)
  )
}

case class TransactionGossip(tx: Transaction, path: Set[Schema.Id]) {
  def hash: String = tx.hash
}

object TransactionGossip {
  def apply(tcd: TransactionCacheData): TransactionGossip = TransactionGossip(tcd.transaction, tcd.path)
  def apply(tx: Transaction): TransactionGossip = TransactionGossip(tx, Set())
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

sealed trait TransactionValidation {
  def errorMessage: String
}

case class InvalidSourceSignature(txHash: String) extends TransactionValidation {
  def errorMessage: String = s"Transaction tx=$txHash has invalid source signature"
}

object InvalidSourceSignature {
  def apply(tx: Transaction) = new InvalidSourceSignature(tx.hash)
}

case class EmptyDestinationAddress(txHash: String) extends TransactionValidation {
  def errorMessage: String = s"Transaction tx=$txHash has an empty destination address"
}

object EmptyDestinationAddress {
  def apply(tx: Transaction) = new EmptyDestinationAddress(tx.hash)
}

case class InvalidDestinationAddress(txHash: String, address: String) extends TransactionValidation {
  def errorMessage: String = s"Transaction tx=$txHash has an invalid destination address=$address"
}

object InvalidDestinationAddress {
  def apply(tx: Transaction) = new InvalidDestinationAddress(tx.hash, tx.dst.hash)
}

case class NonPositiveAmount(txHash: String, amount: Long) extends TransactionValidation {
  def errorMessage: String = s"Transaction tx=$txHash has a non-positive amount=${amount.toString}"
}

object NonPositiveAmount {
  def apply(tx: Transaction) = new NonPositiveAmount(tx.hash, tx.amount)
}

case class HashDuplicateFound(txHash: String) extends TransactionValidation {
  def errorMessage: String = s"Transaction tx=$txHash already exists"
}

object HashDuplicateFound {
  def apply(tx: Transaction) = new HashDuplicateFound(tx.hash)
}

sealed trait TransactionValidatorNel {
  type ValidationResult[A] = ValidatedNel[TransactionValidation, A]

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
    if (tx.dst.address.length > 30 && tx.dst.address.startsWith("DAG"))
      tx.validNel
    else
      InvalidDestinationAddress(tx).invalidNel

  def validateAmount(tx: Transaction): ValidationResult[Transaction] =
    if (tx.amount > 0) tx.validNel else NonPositiveAmount(tx).invalidNel

  import org.constellation.datastore.swaydb.SwayDbConversions._

  // TODO: get rid of unsafeRunSync() and make whole validation async with IO[ValidationResult[Transaction]]
  def validateDuplicate(tx: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
    //if (dao.transactionService.contains(tx.hash).unsafeRunSync())
    if (dao.transactionService.isAccepted(tx.hash).unsafeRunSync())
      HashDuplicateFound(tx).invalidNel
    else
      tx.validNel

  def validateTransaction(tx: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
    validateSourceSignature(tx)
      .product(validateEmptyDestinationAddress(tx))
      .product(validateDestinationAddress(tx))
      .product(validateAmount(tx))
      .product(validateDuplicate(tx))
      .map(_ ⇒ tx)
}

object TransactionValidatorNel extends TransactionValidatorNel
