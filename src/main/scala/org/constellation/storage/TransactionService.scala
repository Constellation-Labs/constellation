package org.constellation.storage

import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.DAO
import org.constellation.primitives.TransactionCacheData
import org.constellation.primitives.concurrency.SingleLock
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}
import org.constellation.storage.transactions.TransactionStatus.TransactionStatus
import org.constellation.storage.transactions.{PendingTransactionsMemPool, TransactionStatus}

class TransactionService[F[_]: Sync: Concurrent](dao: DAO, pullSemaphore: Semaphore[F])
    extends MerkleStorageAlgebra[F, String, TransactionCacheData]
    with StrictLogging {

  val merklePool = new StorageService[F, Seq[String]]()

  private val pending = new PendingTransactionsMemPool[F]()
  private val arbitrary = new StorageService[F, TransactionCacheData](Some(240))
  private val inConsensus = new StorageService[F, TransactionCacheData](Some(240))
  private val accepted = new StorageService[F, TransactionCacheData](Some(240))
  private val unknown = new StorageService[F, TransactionCacheData](Some(240))

  def getArbitrary = arbitrary.toMap()

  def put(tx: TransactionCacheData): F[TransactionCacheData] = put(tx, TransactionStatus.Pending)

  def put(tx: TransactionCacheData, as: TransactionStatus): F[TransactionCacheData] = as match {
    case TransactionStatus.Pending   => pending.put(tx.transaction.hash, tx)
    case TransactionStatus.Arbitrary => arbitrary.put(tx.transaction.hash, tx)
    case TransactionStatus.Accepted  => accepted.put(tx.transaction.hash, tx)
    case TransactionStatus.Unknown   => unknown.put(tx.transaction.hash, tx)
    case _                           => new Exception("Unknown transaction status").raiseError[F, TransactionCacheData]
  }

  def accept(tx: TransactionCacheData): F[Unit] =
    accepted.put(tx.transaction.hash, tx) *>
      inConsensus.remove(tx.transaction.hash) *>
      unknown.remove(tx.transaction.hash) *>
      arbitrary
        .remove(tx.transaction.hash)
        .flatTap(_ => Sync[F].delay(dao.metrics.incrementMetric("transactionAccepted")))

  def lookup(key: String): F[Option[TransactionCacheData]] =
    Lookup.extendedLookup[F, String, TransactionCacheData](List(accepted, arbitrary, inConsensus, pending, unknown))(
      key
    )

  def lookup(hash: String, status: TransactionStatus): F[Option[TransactionCacheData]] =
    status match {
      case TransactionStatus.Pending     => pending.lookup(hash)
      case TransactionStatus.Arbitrary   => arbitrary.lookup(hash)
      case TransactionStatus.InConsensus => inConsensus.lookup(hash)
      case TransactionStatus.Accepted    => accepted.lookup(hash)
      case TransactionStatus.Unknown     => unknown.lookup(hash)
      case _                             => new Exception("Unknown transaction status").raiseError[F, Option[TransactionCacheData]]
    }

  def contains(key: String): F[Boolean] =
    Lookup.extendedContains[F, String, TransactionCacheData](List(accepted, arbitrary, inConsensus, pending, unknown))(
      key
    )

  def isAccepted(hash: String): F[Boolean] = accepted.contains(hash)

  def applySnapshot(txs: List[TransactionCacheData], merkleRoot: String): F[Unit] =
    merklePool.remove(merkleRoot) *>
      txs.map(tx => accepted.remove(tx.transaction.hash)).sequence.void

  def returnTransactionsToPending(txs: Seq[String]): F[List[TransactionCacheData]] =
    txs.toList
      .traverse(inConsensus.lookup)
      .map(_.flatten)
      .flatMap(_.traverse(tx => pending.put(tx.transaction.hash, tx)))

  def pullForConsensusSafe(minCount: Int, roundId: String = "roundId"): F[List[TransactionCacheData]] =
    new SingleLock[F, List[TransactionCacheData]](roundId, pullSemaphore).use(pullForConsensus(minCount, roundId))

  def pullForConsensus(minCount: Int, roundId: String = "roundId"): F[List[TransactionCacheData]] =
    pending
      .pull(minCount)
      .map(_.getOrElse(List()))
      .flatTap(txs => Sync[F].delay(logger.info(s"Pulling txs=${txs.size} for consensus with minCount: $minCount")))
      .flatMap(
        txs => txs.map(tx => inConsensus.put(tx.transaction.hash, tx)).sequence
      )

  def getLast20Accepted: F[List[TransactionCacheData]] =
    accepted.getLast20()

  def findHashesByMerkleRoot(merkleRoot: String): F[Option[Seq[String]]] =
    merklePool.lookup(merkleRoot)

  def count: F[Long] =
    List(
      count(TransactionStatus.Pending),
      count(TransactionStatus.Arbitrary),
      count(TransactionStatus.InConsensus),
      count(TransactionStatus.Accepted),
      count(TransactionStatus.Unknown)
    ).sequence.map(_.combineAll)

  def count(status: TransactionStatus): F[Long] = status match {
    case TransactionStatus.Pending     => pending.size()
    case TransactionStatus.Arbitrary   => arbitrary.size()
    case TransactionStatus.InConsensus => inConsensus.size()
    case TransactionStatus.Accepted    => accepted.size()
    case TransactionStatus.Unknown     => unknown.size()
  }

  def getMetricsMap: F[Map[String, Long]] =
    List(
      count(TransactionStatus.Pending),
      count(TransactionStatus.Arbitrary),
      count(TransactionStatus.InConsensus),
      count(TransactionStatus.Accepted),
      count(TransactionStatus.Unknown)
    ).sequence
      .map(
        counts => {
          Map(
            "pending" -> counts.get(0).getOrElse(0),
            "arbitrary" -> counts.get(1).getOrElse(0),
            "inConsensus" -> counts.get(2).getOrElse(0),
            "accepted" -> counts.get(3).getOrElse(0),
            "unknown" -> counts.get(4).getOrElse(0)
          )
        }
      )
}
