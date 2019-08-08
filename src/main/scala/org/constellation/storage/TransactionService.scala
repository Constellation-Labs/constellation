package org.constellation.storage

import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.TransactionCacheData
import org.constellation.primitives.concurrency.SingleLock
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}
import org.constellation.storage.transactions.TransactionStatus.TransactionStatus
import org.constellation.storage.transactions.{PendingTransactionsMemPool, TransactionStatus}
import org.constellation.{ConstellationContextShift, DAO}

class TransactionService[F[_]: Concurrent](dao: DAO)
    extends MerkleStorageAlgebra[F, String, TransactionCacheData]
    with StrictLogging {

  val merklePool = new StorageService[F, Seq[String]]()

  val semaphore: Semaphore[F] = {
    implicit val cs: ContextShift[IO] = ConstellationContextShift.edge
    Semaphore.in[IO, F](1).unsafeRunSync()
  }

  private[storage] val pending = new PendingTransactionsMemPool[F]()
  private[storage] val arbitrary = new StorageService[F, TransactionCacheData](Some(240))
  private[storage] val inConsensus = new StorageService[F, TransactionCacheData](Some(240))
  private[storage] val accepted = new StorageService[F, TransactionCacheData](Some(240))
  private[storage] val unknown = new StorageService[F, TransactionCacheData](Some(240))

  val semaphores = Map(
    "arbitraryUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "inConsensusUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "acceptedUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "unknownUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "merklePoolUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync()
  )

  private def withLock[R](semaphoreName: String, thunk: F[R]) =
    new SingleLock[F, R](semaphoreName, semaphores(semaphoreName)).use(thunk)

  def getArbitrary = arbitrary.toMap()

  def put(tx: TransactionCacheData): F[TransactionCacheData] = put(tx, TransactionStatus.Pending)

  def put(tx: TransactionCacheData, as: TransactionStatus): F[TransactionCacheData] = as match {
    case TransactionStatus.Pending   => pending.put(tx.transaction.hash, tx)
    case TransactionStatus.Arbitrary => withLock("arbitraryUpdate", arbitrary.put(tx.transaction.hash, tx))
    case TransactionStatus.Accepted  => withLock("acceptedUpdate", accepted.put(tx.transaction.hash, tx))
    case TransactionStatus.Unknown   => withLock("unknownUpdate", unknown.put(tx.transaction.hash, tx))
    case _                           => new Exception("Unknown transaction status").raiseError[F, TransactionCacheData]
  }

  def update(key: String, fn: TransactionCacheData => TransactionCacheData): F[Option[TransactionCacheData]] =
    for {
      p <- pending.update(key, fn)
      i <- p.fold(withLock("inConsensusUpdate", inConsensus.update(key, fn)))(curr => Sync[F].pure(Some(curr)))
      ac <- i.fold(withLock("acceptedUpdate", accepted.update(key, fn)))(curr => Sync[F].pure(Some(curr)))
      a <- ac.fold(withLock("arbitraryUpdate", arbitrary.update(key, fn)))(curr => Sync[F].pure(Some(curr)))
      result <- a.fold(withLock("unknownUpdate", unknown.update(key, fn)))(curr => Sync[F].pure(Some(curr)))
    } yield result

  def accept(tx: TransactionCacheData): F[Unit] =
    put(tx, TransactionStatus.Accepted) *>
      withLock("inConsensusUpdate", inConsensus.remove(tx.transaction.hash)) *>
      withLock("unknownUpdate", unknown.remove(tx.transaction.hash)) *>
      withLock("arbitraryUpdate", arbitrary.remove(tx.transaction.hash))
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
    withLock("merklePoolUpdate", merklePool.remove(merkleRoot)) *>
      txs.traverse(tx => withLock("acceptedUpdate", accepted.remove(tx.transaction.hash))).void

  def applySnapshot(merkleRoot: String): F[Unit] =
    findHashesByMerkleRoot(merkleRoot).flatMap(tx => withLock("acceptedUpdate", accepted.remove(tx.toSet.flatten))) *>
      withLock("merklePoolUpdate", merklePool.remove(merkleRoot))

  def returnTransactionsToPending(txs: Seq[String]): F[List[TransactionCacheData]] =
    txs.toList
      .traverse(inConsensus.lookup)
      .map(_.flatten)
      .flatMap { txs =>
        txs.traverse(tx => withLock("inConsensusUpdate", inConsensus.remove(tx.transaction.hash))) *>
          txs.traverse(tx => put(tx))
      }

  def pullForConsensus(minCount: Int, roundId: String = "roundId"): F[List[TransactionCacheData]] =
    pending
      .pull(minCount)
      .map(_.getOrElse(List()))
      .flatTap(txs => Sync[F].delay(logger.debug(s"Pulling txs=${txs.size} for consensus with minCount: $minCount")))
      .flatMap(x => x.traverse(tx => withLock("inConsensusUpdate", inConsensus.put(tx.transaction.hash, tx))))

  def pullForConsensusWithDummy(minCount: Int, roundId: String = "roundId"): F[List[TransactionCacheData]] =
    count(status = TransactionStatus.Pending).flatMap {
      case 0L => dummyTransaction(minCount, roundId)
      case _  => pullForConsensus(minCount, roundId)
    }

  def dummyTransaction(minCount: Int, roundId: String): F[List[TransactionCacheData]] =
    List
      .fill(minCount)(
        TransactionCacheData(
          createDummyTransaction(dao.selfAddressStr, KeyUtils.makeKeyPair().getPublic.toId.address, dao.keyPair)
        )
      )
      .traverse(put)
      .flatMap(_ => pullForConsensus(minCount, roundId))

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
