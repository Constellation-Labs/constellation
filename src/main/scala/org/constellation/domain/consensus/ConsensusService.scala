package org.constellation.domain.consensus

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, IO, Sync}
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext.createSemaphore
import org.constellation.domain.consensus.ConsensusStatus.ConsensusStatus
import org.constellation.schema.checkpoint.CheckpointCache
import org.constellation.concurrency.SingleLock
import org.constellation.schema.consensus.ConsensusObject
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}
import org.constellation.storage.{ConcurrentStorageService, PendingMemPool, StorageService}

abstract class ConsensusService[F[_]: Concurrent, A <: ConsensusObject] extends MerkleStorageAlgebra[F, String, A] {

  def metricRecordPrefix: Option[String]

  private val logger = Slf4jLogger.getLogger[F]

  protected[domain] val merklePool =
    new ConcurrentStorageService[F, Seq[String]](createSemaphore(), metricRecordPrefix.map(_ + "_merklePool"))

  val semaphores: Map[String, Semaphore[F]] = Map(
    "inConsensusUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "acceptedUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "unknownUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "pendingUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "merklePoolUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync()
  )

  protected[domain] def withLock[R](semaphoreName: String, thunk: F[R]): F[R] =
    new SingleLock[F, R](semaphoreName, semaphores(semaphoreName))
      .use(thunk)

  protected[domain] val pending: PendingMemPool[F, String, A]
  protected[domain] val inConsensus = new StorageService[F, A](metricRecordPrefix.map(_ + "_inConsensus"))
  protected[domain] val accepted = new StorageService[F, A](metricRecordPrefix.map(_ + "_accepted"))
  protected[domain] val unknown = new StorageService[F, A](metricRecordPrefix.map(_ + "_unknown"))

  def applySnapshot(a: List[A], merkleRoot: String): F[Unit] =
    withLock("merklePoolUpdate", merklePool.remove(merkleRoot)) >>
      a.traverse(x => withLock("acceptedUpdate", accepted.remove(x.hash))).void

  def applySnapshotDirect(a: List[A]): F[Unit] =
    a.traverse(x => withLock("acceptedUpdate", accepted.remove(x.hash))).void

  def removeMerkleRoot(merkleRoot: String): F[Unit] =
    findHashesByMerkleRoot(merkleRoot).flatMap(a => withLock("acceptedUpdate", accepted.remove(a.toSet.flatten))) >>
      withLock("merklePoolUpdate", merklePool.remove(merkleRoot))

  def remove(hash: String): F[Unit] =
    accepted.remove(hash)

  def put(a: A): F[A] = put(a, ConsensusStatus.Pending)

  def put(a: A, as: ConsensusStatus, cpc: Option[CheckpointCache] = None): F[A] = as match {
    case ConsensusStatus.Pending =>
      pending
        .put(a.hash, a)
        .flatTap(
          _ =>
            logger.debug(s"ConsensusService pendingPut with hash=${a.hash} - with checkpoint hash=${cpc
              .map(_.checkpointBlock.baseHash)}")
        )
    case ConsensusStatus.Accepted =>
      withLock("acceptedUpdate", accepted.put(a.hash, a))
        .flatTap(
          _ =>
            logger.debug(s"ConsensusService acceptedPut with hash=${a.hash} - with checkpoint hash=${cpc
              .map(_.checkpointBlock.baseHash)}")
        )
    case ConsensusStatus.Unknown =>
      withLock("unknownUpdate", unknown.put(a.hash, a))
        .flatTap(
          _ =>
            logger.debug(s"ConsensusService unknownPut with hash=${a.hash} - with checkpoint hash=${cpc
              .map(_.checkpointBlock.baseHash)}")
        )
    case _ => new Exception("Unknown consensus status").raiseError[F, A]
  }

  def update(key: String, fn: A => A, empty: => A, as: ConsensusStatus): F[A] = as match {
    case ConsensusStatus.Pending =>
      pending.update(key, fn, empty).flatTap(_ => logger.debug(s"ConsensusService pendingUpdate with hash=${key}"))
    case ConsensusStatus.InConsensus =>
      withLock("inConsensusUpdate", inConsensus.update(key, fn, empty))
        .flatTap(_ => logger.debug(s"ConsensusService inConsensusUpdate with hash=${key}"))
    case ConsensusStatus.Accepted =>
      withLock("acceptedUpdate", accepted.update(key, fn, empty))
        .flatTap(_ => logger.debug(s"ConsensusService acceptedUpdate with hash=${key}"))
    case ConsensusStatus.Unknown =>
      withLock("unknownUpdate", unknown.update(key, fn, empty))
        .flatTap(_ => logger.debug(s"ConsensusService unknownUpdate with hash=${key}"))

    case _ => new Exception("Unknown consensus status").raiseError[F, A]
  }

  def update(key: String, fn: A => A): F[Option[A]] =
    for {
      p <- pending.update(key, fn).flatTap(_ => logger.debug(s"ConsensusService pendingUpdate with hash=${key}"))
      i <- p
        .fold(
          logger.debug(s"ConsensusService inConsensusUpdate with hash=${key}") >> withLock(
            "inConsensusUpdate",
            inConsensus.update(key, fn)
          )
        )(curr => Sync[F].pure(Some(curr)))
      ac <- i
        .fold(
          logger.debug(s"ConsensusService acceptedUpdate with hash=${key}") >> withLock(
            "acceptedUpdate",
            accepted.update(key, fn)
          )
        )(curr => Sync[F].pure(Some(curr)))
      result <- ac
        .fold(
          logger.debug(s"ConsensusService unknownUpdate with hash=${key}") >> withLock(
            "unknownUpdate",
            unknown.update(key, fn)
          )
        )(curr => Sync[F].pure(Some(curr)))
    } yield result

  def accept(a: A, cpc: Option[CheckpointCache] = None): F[Unit] =
    put(a, ConsensusStatus.Accepted, cpc) >>
      withLock("inConsensusUpdate", inConsensus.remove(a.hash)) >>
      withLock("unknownUpdate", unknown.remove(a.hash)) >>
      withLock("pendingUpdate", pending.remove(a.hash))
        .flatTap(
          _ =>
            logger.debug(s"ConsensusService remove with hash=${a.hash} - with checkpoint hash=${cpc
              .map(_.checkpointBlock.baseHash)}")
        )

  def isAccepted(hash: String): F[Boolean] = accepted.contains(hash)

  def pullForConsensus(count: Int): F[List[A]] =
    pending
      .pull(count)
      .map(_.getOrElse(List()))
      .flatMap(
        _.traverse(
          a =>
            withLock("inConsensusUpdate", inConsensus.put(a.hash, a))
              .flatTap(_ => logger.debug(s"ConsensusService pulling for consensus with hash=${a.hash}"))
        )
      )

  def findByPredicate(predicate: A => Boolean): F[List[A]] = {
    val filter: Map[String, A] => List[A] = _.filter { case (_, v) => predicate(v) }.values.toList

    for {
      p <- pending.toMap().map(filter)
      a <- accepted.toMap().map(filter)
      ic <- inConsensus.toMap().map(filter)
    } yield p ++ a ++ ic
  }

  def lookup(key: String): F[Option[A]] =
    Lookup.extendedLookup[F, String, A](List(accepted, inConsensus, pending, unknown))(
      key
    )

  def lookup(hash: String, status: ConsensusStatus): F[Option[A]] =
    status match {
      case ConsensusStatus.Pending     => pending.lookup(hash)
      case ConsensusStatus.InConsensus => inConsensus.lookup(hash)
      case ConsensusStatus.Accepted    => accepted.lookup(hash)
      case ConsensusStatus.Unknown     => unknown.lookup(hash)
      case _                           => new Exception("Unknown consensus status").raiseError[F, Option[A]]
    }

  def contains(key: String): F[Boolean] =
    Lookup.extendedContains[F, String, A](List(accepted, inConsensus, pending, unknown))(
      key
    )

  def clearInConsensus(as: Seq[String]): F[List[A]] =
    as.toList
      .traverse(inConsensus.lookup)
      .map(_.flatten)
      .flatMap { txs =>
        txs.traverse(tx => withLock("inConsensusUpdate", inConsensus.remove(tx.hash))) >>
          txs.traverse(tx => put(tx, ConsensusStatus.Unknown))
      }
      .flatTap(txs => logger.debug(s"ConsensusService clear and add to unknown  with hashes=${txs.map(_.hash)}"))

  def returnToPending(as: Seq[String]): F[List[A]] =
    as.toList
      .traverse(inConsensus.lookup)
      .map(_.flatten)
      .flatMap { txs =>
        txs.traverse(tx => withLock("inConsensusUpdate", inConsensus.remove(tx.hash))) >>
          txs.traverse(tx => put(tx))
      }
      .flatTap(
        txs =>
          if (txs.nonEmpty) logger.info(s"ConsensusService returningToPending with hashes=${txs.map(_.hash)}")
          else Sync[F].unit
      )

  def clear: F[Unit] =
    for {
      _ <- pending.clear
      _ <- inConsensus.clear
      _ <- accepted.clear
      _ <- unknown.clear
    } yield ()

  def getLast20Accepted: F[List[A]] =
    accepted.getLast20()

  override def findHashesByMerkleRoot(merkleRoot: String): F[Option[Seq[String]]] =
    merklePool.lookup(merkleRoot)

  override def addMerkle(merkleRoot: String, keys: Seq[String]): F[Seq[String]] = merklePool.put(merkleRoot, keys)

  def count: F[Long] =
    List(
      count(ConsensusStatus.Pending),
      count(ConsensusStatus.InConsensus),
      count(ConsensusStatus.Accepted),
      count(ConsensusStatus.Unknown)
    ).sequence.map(_.combineAll)

  def count(status: ConsensusStatus): F[Long] = status match {
    case ConsensusStatus.Pending     => pending.size()
    case ConsensusStatus.InConsensus => inConsensus.size()
    case ConsensusStatus.Accepted    => accepted.size()
    case ConsensusStatus.Unknown     => unknown.size()
  }

  def getMetricsMap: F[Map[String, Long]] =
    List(
      count(ConsensusStatus.Pending),
      count(ConsensusStatus.InConsensus),
      count(ConsensusStatus.Accepted),
      count(ConsensusStatus.Unknown)
    ).sequence
      .map(
        counts => {
          Map(
            "pending" -> counts.get(0).getOrElse(0),
            "inConsensus" -> counts.get(1).getOrElse(0),
            "accepted" -> counts.get(2).getOrElse(0),
            "unknown" -> counts.get(3).getOrElse(0)
          )
        }
      )
}
