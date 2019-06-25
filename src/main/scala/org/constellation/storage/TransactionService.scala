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

class TransactionService[F[_]: Sync: Concurrent](dao: DAO, semaphore: Semaphore[F])
    extends MerkleStorageAlgebra[F, String, TransactionCacheData]
    with StrictLogging {

  val merklePool = new StorageService[F, Seq[String]]()

  private[storage] val pending = new PendingTransactionsMemPool[F]()
  private[storage] val arbitrary = new StorageService[F, TransactionCacheData](Some(240))
  private[storage] val inConsensus = new StorageService[F, TransactionCacheData](Some(240))
  private[storage] val accepted = new StorageService[F, TransactionCacheData](Some(240))
  private[storage] val unknown = new StorageService[F, TransactionCacheData](Some(240))

  private def withLock[R](name: String, thunk: F[R]) = new SingleLock[F, R](name, semaphore).use(thunk)

  def getArbitrary = arbitrary.toMap()

  def put(tx: TransactionCacheData): F[TransactionCacheData] = put(tx, TransactionStatus.Pending)

  def put(tx: TransactionCacheData, as: TransactionStatus): F[TransactionCacheData] = as match {
    case TransactionStatus.Pending   => withLock("pendingPut", pending.put(tx.transaction.hash, tx))
    case TransactionStatus.Arbitrary => withLock("arbitraryPut", arbitrary.put(tx.transaction.hash, tx))
    case TransactionStatus.Accepted  => withLock("acceptedPut", accepted.put(tx.transaction.hash, tx))
    case TransactionStatus.Unknown   => withLock("unknownPut", unknown.put(tx.transaction.hash, tx))
    case _                           => new Exception("Unknown transaction status").raiseError[F, TransactionCacheData]
  }

  def update(key: String, fn: TransactionCacheData => TransactionCacheData): F[Unit] =
    for {
      _ <- withLock("pendingUpdate", pending.update(key, fn))
      _ <- withLock("arbitraryUpdate", arbitrary.update(key, fn))
      _ <- withLock("inConsensusUpdate", inConsensus.update(key, fn))
      _ <- withLock("acceptedUpdate", accepted.update(key, fn))
      _ <- withLock("unknownUpdate", unknown.update(key, fn))
    } yield ()

  def accept(tx: TransactionCacheData): F[Unit] =
    put(tx, TransactionStatus.Accepted) *>
      withLock("inConsensusRemove", inConsensus.remove(tx.transaction.hash)) *>
      withLock("unknownRemove", unknown.remove(tx.transaction.hash)) *>
      withLock("arbitraryRemove", arbitrary.remove(tx.transaction.hash))
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
    withLock("merklePoolRemove", merklePool.remove(merkleRoot)) *>
      txs.traverse(tx => withLock("acceptedRemove", accepted.remove(tx.transaction.hash))).void

  def returnTransactionsToPending(txs: Seq[String]): F[List[TransactionCacheData]] =
    txs.toList
      .traverse(inConsensus.lookup)
      .map(_.flatten)
      .flatMap { txs =>
        txs.traverse(tx => withLock("inConsensusRemove", inConsensus.remove(tx.transaction.hash))) *>
          txs.traverse(tx => put(tx))
      }

  def pullForConsensus(minCount: Int, roundId: String = "roundId"): F[List[TransactionCacheData]] =
    withLock("pullForConsensus", pending.pull(minCount))
      .map(_.getOrElse(List()))
      .flatTap(txs => Sync[F].delay(logger.debug(s"Pulling txs=${txs.size} for consensus with minCount: $minCount")))
      .flatMap(_.traverse(tx => withLock("inConsensusPut", inConsensus.put(tx.transaction.hash, tx))))

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
