package org.constellation.schema.transaction

import java.security.KeyPair
import java.time.Instant

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.schema.Id
import org.constellation.schema.address.Address
import org.constellation.schema.consensus.ConsensusObject
import org.constellation.schema.edge.Edge
import org.constellation.schema.signature.HashSignature

import org.constellation.schema._

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

  implicit val transactionCacheDataEncoder: Encoder[TransactionCacheData] = deriveEncoder
  implicit val transactionCacheDataDecoder: Decoder[TransactionCacheData] = deriveDecoder
}

case class Transaction(
  edge: Edge[TransactionEdgeData],
  lastTxRef: LastTransactionRef,
  isDummy: Boolean = false,
  isTest: Boolean = false
) {

  def src: Address = Address(edge.parents.head.hashReference)

  def dst: Address = Address(edge.parents.last.hashReference)

  def signatures: Seq[HashSignature] = edge.signedObservationEdge.signatureBatch.signatures

  // TODO: Add proper exception on empty option

  def amount: Long = edge.data.amount

  def fee: Option[Long] = edge.data.fee

  def baseHash: String = edge.signedObservationEdge.baseHash

  def hash: String = edge.observationEdge.hash

  def signaturesHash: String = edge.signedObservationEdge.signatureBatch.hash

  def withSignatureFrom(keyPair: KeyPair): Transaction = this.copy(
    edge = edge.withSignatureFrom(keyPair)
  )

  def isValid = signatures.exists { hs ⇒
    hs.publicKey.address == src.address && hs.valid(signaturesHash) && hash == signaturesHash
  }

  def feeValue: Long = fee.map(v => if (v < 0L) 0L else v).getOrElse(0L)

  val ordinal: Long = lastTxRef.ordinal + 1L
}

object Transaction {
  implicit val edgeTransactionDataEncoder: Encoder[Edge[TransactionEdgeData]] = deriveEncoder
  implicit val edgeTransactionDataDecoder: Decoder[Edge[TransactionEdgeData]] = deriveDecoder

  implicit val transactionEncoder: Encoder[Transaction] = deriveEncoder
  implicit val transactionDecoder: Decoder[Transaction] = deriveDecoder
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
  isDummy: Boolean,
  isTest: Boolean
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
      tx.isDummy,
      tx.isTest
    )
}
