package org.constellation.primitives.storage

import cats.effect._
import cats.effect.concurrent._
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.DAO
import org.constellation.primitives.TransactionCacheData
import org.constellation.primitives.storage.TransactionStatus.TransactionStatus

import scala.concurrent.ExecutionContext

object TransactionStatus extends Enumeration {
  type TransactionStatus = Value
  val Pending, Arbitrary, InConsensus, Accepted, Unknown = Value
}

trait TransactionService[H, T] extends MerkleService[H, T] {
  val merklePool: StorageService[Seq[H]]
  def put(tx: T): IO[T] = put(tx, as = TransactionStatus.Pending, overrideLimit = false)
  def put(tx: T, overrideLimit: Boolean): IO[T] = put(tx, TransactionStatus.Pending, overrideLimit)
  def put(tx: T, as: TransactionStatus, overrideLimit: Boolean): IO[T]
  def accept(tx: T): IO[Unit]
  def lookup: H => IO[Option[T]]
  def lookup(hash: H, status: TransactionStatus): IO[Option[T]]
  def exists: H => IO[Boolean]
  def isAccepted(hash: H): IO[Boolean]
  def pullForConsensus(minCount: Int): IO[List[T]]
  def applySnapshot(txs: List[T]): IO[Unit]
  def getArbitrary: IO[Map[H, T]]
  def getLast20Accepted: IO[List[T]]
  def findHashesByMerkleRoot(merkleRoot: H): IO[Option[Seq[H]]]
  def count: IO[Long]
  def count(status: TransactionStatus): IO[Long]

  def getMetricsMap: IO[Map[String, Long]]
}

class PendingTransactionsMemPool(implicit dao: DAO) extends Lookup[String, TransactionCacheData] {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val transactions: Ref[IO, Map[String, TransactionCacheData]] =
    Ref.of[IO, Map[String, TransactionCacheData]](Map())
      .unsafeRunSync()

  def put(key: String, value: TransactionCacheData, overrideLimit: Boolean): IO[TransactionCacheData] =
    transactions.get
      .flatMap(txs => transactions.set(txs + (key -> value)))
      .map(_ => value)

  def lookup(key: String): IO[Option[TransactionCacheData]] = transactions
    .get
    .map(_.find(_._2.transaction.hash == key).map(_._2))

  def contains(key: String): IO[Boolean] = transactions
    .get
    .map(_.exists(_._2.transaction.hash == key))

  // TODO: Rethink - use queue
  def pull(minCount: Int): IO[Option[List[TransactionCacheData]]] = transactions.get.flatMap { txs =>
    if (txs.size < minCount) IO.pure(None)
    else {
      val (left, right) = txs.splitAt(minCount)
      transactions.set(right).map(_ => Some(left.toList.map(_._2)))
    }
  }

  def size: Int = transactions.get.map(_.size).unsafeRunSync()
}

class TransactionMemPool() extends StorageService[TransactionCacheData](Some(240))

class DefaultTransactionService(dao: DAO) extends TransactionService[String, TransactionCacheData] with StrictLogging {
  val merklePool = new StorageService[Seq[String]]()

  val pending = new PendingTransactionsMemPool()(dao)
  val arbitrary = new TransactionMemPool()
  val inConsensus = new TransactionMemPool()
  val accepted = new TransactionMemPool()
  val unknown = new TransactionMemPool()

  def getArbitrary = arbitrary.toMap()

  def put(tx: TransactionCacheData, as: TransactionStatus, overrideLimit: Boolean): IO[TransactionCacheData] = as match {
    case TransactionStatus.Pending => pending.put(tx.transaction.hash, tx, overrideLimit)
    case TransactionStatus.Arbitrary => arbitrary.put(tx.transaction.hash, tx)
    case TransactionStatus.Accepted => accepted.put(tx.transaction.hash, tx)
    case TransactionStatus.Unknown => unknown.put(tx.transaction.hash, tx)
    case _ => IO.raiseError(new Exception("Unknown transaction status"))
  }

  def accept(tx: TransactionCacheData): IO[Unit] =
    accepted.put(tx.transaction.hash, tx) *>
      inConsensus.remove(tx.transaction.hash) *>
      unknown.remove(tx.transaction.hash) *>
      arbitrary.remove(tx.transaction.hash)
        .flatTap(_ => dao.metrics.incrementMetricAsync("transactionAccepted"))


  def lookup: String => IO[Option[TransactionCacheData]] =
    DbStorage.extendedLookup[String, TransactionCacheData](List(accepted, arbitrary, inConsensus, pending, unknown))

  def lookup(hash: String, status: TransactionStatus): IO[Option[TransactionCacheData]] = status match {
    case TransactionStatus.Pending => pending.lookup(hash)
    case TransactionStatus.Arbitrary => arbitrary.lookup(hash)
    case TransactionStatus.InConsensus => inConsensus.lookup(hash)
    case TransactionStatus.Accepted => accepted.lookup(hash)
    case TransactionStatus.Unknown => unknown.lookup(hash)
    case _ => IO.raiseError(new Exception("Unknown transaction status"))
  }

  def exists: String => IO[Boolean] =
    DbStorage.extendedContains[String, TransactionCacheData](List(accepted, arbitrary, inConsensus, pending, unknown))


  def isAccepted(hash: String): IO[Boolean] = accepted.contains(hash)

  def applySnapshot(txs: List[TransactionCacheData]): IO[Unit] =
    txs.map(tx => accepted.remove(tx.transaction.hash)).sequence.void

  def pullForConsensus(minCount: Int): IO[List[TransactionCacheData]] =
    pending.pull(minCount)
      .map(_.getOrElse(List()))
      .flatMap(txs => txs.map(tx => inConsensus.put(tx.transaction.hash, tx)).sequence[IO, TransactionCacheData])

  def getLast20Accepted: IO[List[TransactionCacheData]] =
    accepted.getLast20

  def findHashesByMerkleRoot(merkleRoot: String): IO[Option[Seq[String]]] = merklePool.get(merkleRoot)

  def count: IO[Long] = List(
    count(TransactionStatus.Pending),
    count(TransactionStatus.Arbitrary),
    count(TransactionStatus.InConsensus),
    count(TransactionStatus.Accepted),
    count(TransactionStatus.Unknown)
  ).sequence.map(_.combineAll)

  def count(status: TransactionStatus): IO[Long] = status match {
    case TransactionStatus.Pending => IO(pending.size.toLong)
    case TransactionStatus.Arbitrary => IO(arbitrary.cacheSize())
    case TransactionStatus.InConsensus => IO(inConsensus.cacheSize())
    case TransactionStatus.Accepted => IO(accepted.cacheSize())
    case TransactionStatus.Unknown => IO(unknown.cacheSize())
  }

  def getMetricsMap: IO[Map[String, Long]] =
    List(
      count(TransactionStatus.Pending),
      count(TransactionStatus.Arbitrary),
      count(TransactionStatus.InConsensus),
      count(TransactionStatus.Accepted),
      count(TransactionStatus.Unknown)
    )
      .sequence
      .flatMap(counts => IO {
        Map(
          "pending" -> counts.get(0).getOrElse(0),
          "arbitrary" -> counts.get(1).getOrElse(0),
          "inConsensus" -> counts.get(2).getOrElse(0),
          "accepted" -> counts.get(3).getOrElse(0),
          "unknown" -> counts.get(4).getOrElse(0)
        )
      })
}
