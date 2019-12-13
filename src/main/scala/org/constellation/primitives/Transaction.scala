package org.constellation.primitives

import java.security.KeyPair
import java.time.Instant

import cats.data.ValidatedNel
import cats.implicits._
import constellation._
import org.constellation.DAO
import org.constellation.domain.consensus.ConsensusObject
import org.constellation.primitives.Schema.{Address, TransactionEdgeData}
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.schema.Id
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
) extends ConsensusObject {

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

  def hash = transaction.hash
}

object TransactionCacheData {
  def apply(tx: Transaction): TransactionCacheData = TransactionCacheData(transaction = tx)
}

case class Transaction(
  edge: Edge[TransactionEdgeData],
  lastTxRef: LastTransactionRef,
  isDummy: Boolean = false
) {

  def src: Address = Address(edge.parents.head.hash)

  def prevTxOrdinal: Address = Address(edge.parents.tail.head.hash)

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

  val ordinal: Long = lastTxRef.ordinal + 1L
}

case class TransactionGossip(tx: Transaction, path: Set[Id]) {
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
  time: Long,
  isDummy: Boolean
) {}

object TransactionSerialized {

  def apply(tx: Transaction): TransactionSerialized =
    new TransactionSerialized(
      tx.hash,
      tx.src.address,
      tx.dst.address,
      tx.amount,
      tx.signatures.map(_.address).toSet,
      Instant.now.getEpochSecond,
      tx.isDummy
    )
}
