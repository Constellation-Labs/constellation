package org.constellation.storage

import cats.effect.{Concurrent, ContextShift, IO, Sync}
import cats.effect.concurrent.Semaphore
import io.chrisdavenport.log4cats.Logger
import cats.implicits._
import org.constellation.ConstellationExecutionContext
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.primitives.concurrency.SingleLock
import org.constellation.storage.ConsensusStatus.ConsensusStatus
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}

object ConsensusStatus extends Enumeration {
  type ConsensusStatus = Value
  val Pending, Arbitrary, InConsensus, Accepted, Unknown = Value
}

trait ConsensusObject {
  def hash: String
}

abstract class ConsensusService[F[_]: Concurrent: Logger, A <: ConsensusObject]
    extends MerkleStorageAlgebra[F, String, A] {
  val merklePool = new StorageService[F, Seq[String]]()

  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  val semaphores = Map(
    "arbitraryUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "inConsensusUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "acceptedUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "unknownUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync(),
    "merklePoolUpdate" -> Semaphore.in[IO, F](1).unsafeRunSync()
  )

  protected[storage] def withLock[R](semaphoreName: String, thunk: F[R]): F[R] =
    new SingleLock[F, R](semaphoreName, semaphores(semaphoreName))
      .use(thunk)

  protected[storage] val pending: PendingMemPool[F, String, A]
  protected[storage] val arbitrary = new StorageService[F, A](Some(240))
  protected[storage] val inConsensus = new StorageService[F, A](Some(240))
  protected[storage] val accepted = new StorageService[F, A](Some(240))
  protected[storage] val unknown = new StorageService[F, A](Some(240))

  def getArbitrary: F[Map[String, A]] = arbitrary.toMap()

  def put(a: A): F[A] = put(a, ConsensusStatus.Pending)

  def put(a: A, as: ConsensusStatus, cpc: Option[CheckpointCache] = None): F[A] = as match {
    case ConsensusStatus.Pending =>
      pending
        .put(a.hash, a)
        .flatTap(
          _ =>
            Logger[F].debug(s"ConsensusService pendingPut with hash=${a.hash} - with checkpoint hash=${cpc
              .map(c => c.checkpointBlock.map(_.baseHash))}")
        )
    case ConsensusStatus.Arbitrary =>
      withLock("arbitraryUpdate", arbitrary.put(a.hash, a))
        .flatTap(
          _ =>
            Logger[F].debug(s"ConsensusService arbitraryPut with hash=${a.hash} - with checkpoint hash=${cpc
              .map(c => c.checkpointBlock.map(_.baseHash))}")
        )
    case ConsensusStatus.Accepted =>
      withLock("acceptedUpdate", accepted.put(a.hash, a))
        .flatTap(
          _ =>
            Logger[F].debug(s"ConsensusService acceptedPut with hash=${a.hash} - with checkpoint hash=${cpc
              .map(c => c.checkpointBlock.map(_.baseHash))}")
        )
    case ConsensusStatus.Unknown =>
      withLock("unknownUpdate", unknown.put(a.hash, a))
        .flatTap(
          _ =>
            Logger[F].debug(s"ConsensusService unknownPut with hash=${a.hash} - with checkpoint hash=${cpc
              .map(c => c.checkpointBlock.map(_.baseHash))}")
        )
    case _ => new Exception("Unknown consensus status").raiseError[F, A]
  }

  def update(key: String, fn: A => A, empty: => A, as: ConsensusStatus): F[A] = as match {
    case ConsensusStatus.Pending =>
      pending.update(key, fn, empty).flatTap(_ => Logger[F].debug(s"ConsensusService pendingUpdate with hash=${key}"))
    case ConsensusStatus.InConsensus =>
      withLock("inConsensusUpdate", inConsensus.update(key, fn, empty))
        .flatTap(_ => Logger[F].debug(s"ConsensusService inConsensusUpdate with hash=${key}"))
    case ConsensusStatus.Arbitrary =>
      withLock("arbitraryUpdate", arbitrary.update(key, fn, empty))
        .flatTap(_ => Logger[F].debug(s"ConsensusService arbitraryUpdate with hash=${key}"))
    case ConsensusStatus.Accepted =>
      withLock("acceptedUpdate", accepted.update(key, fn, empty))
        .flatTap(_ => Logger[F].debug(s"ConsensusService acceptedUpdate with hash=${key}"))
    case ConsensusStatus.Unknown =>
      withLock("unknownUpdate", unknown.update(key, fn, empty))
        .flatTap(_ => Logger[F].debug(s"ConsensusService unknownUpdate with hash=${key}"))

    case _ => new Exception("Unknown consensus status").raiseError[F, A]
  }

  def update(key: String, fn: A => A): F[Option[A]] =
    for {
      p <- pending.update(key, fn).flatTap(_ => Logger[F].debug(s"ConsensusService pendingUpdate with hash=${key}"))
      i <- p
        .fold(
          Logger[F].debug(s"ConsensusService inConsensusUpdate with hash=${key}") >> withLock(
            "inConsensusUpdate",
            inConsensus.update(key, fn)
          )
        )(curr => Sync[F].pure(Some(curr)))
      ac <- i
        .fold(
          Logger[F].debug(s"ConsensusService acceptedUpdate with hash=${key}") >> withLock(
            "acceptedUpdate",
            accepted.update(key, fn)
          )
        )(curr => Sync[F].pure(Some(curr)))
      a <- ac
        .fold(
          Logger[F].debug(s"ConsensusService arbitraryUpdate with hash=${key}") >> withLock(
            "arbitraryUpdate",
            arbitrary.update(key, fn)
          )
        )(curr => Sync[F].pure(Some(curr)))
      result <- a
        .fold(
          Logger[F].debug(s"ConsensusService unknownUpdate with hash=${key}") >> withLock(
            "unknownUpdate",
            unknown.update(key, fn)
          )
        )(curr => Sync[F].pure(Some(curr)))
    } yield result

  def accept(a: A, cpc: Option[CheckpointCache] = None): F[Unit] =
    put(a, ConsensusStatus.Accepted, cpc) >>
      withLock("inConsensusUpdate", inConsensus.remove(a.hash)) >>
      withLock("unknownUpdate", unknown.remove(a.hash)) >>
      withLock("arbitraryUpdate", arbitrary.remove(a.hash))
        .flatTap(
          _ =>
            Logger[F].debug(s"ConsensusService remove with hash=${a.hash} - with checkpoint hash=${cpc
              .map(c => c.checkpointBlock.map(_.baseHash))}")
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
              .flatTap(_ => Logger[F].debug(s"ConsensusService pulling for consensus with hash=${a.hash}"))
        )
      )

  def lookup(key: String): F[Option[A]] =
    Lookup.extendedLookup[F, String, A](List(accepted, arbitrary, inConsensus, pending, unknown))(
      key
    )

  def lookup(hash: String, status: ConsensusStatus): F[Option[A]] =
    status match {
      case ConsensusStatus.Pending     => pending.lookup(hash)
      case ConsensusStatus.Arbitrary   => arbitrary.lookup(hash)
      case ConsensusStatus.InConsensus => inConsensus.lookup(hash)
      case ConsensusStatus.Accepted    => accepted.lookup(hash)
      case ConsensusStatus.Unknown     => unknown.lookup(hash)
      case _                           => new Exception("Unknown consensus status").raiseError[F, Option[A]]
    }

  def contains(key: String): F[Boolean] =
    Lookup.extendedContains[F, String, A](List(accepted, arbitrary, inConsensus, pending, unknown))(
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
      .flatTap(txs => Logger[F].debug(s"ConsensusService clear and add to unknown  with hashes=${txs.map(_.hash)}"))

  def returnToPending(as: Seq[String]): F[List[A]] =
    as.toList
      .traverse(inConsensus.lookup)
      .map(_.flatten)
      .flatMap { txs =>
        txs.traverse(tx => withLock("inConsensusUpdate", inConsensus.remove(tx.hash))) >>
          txs.traverse(tx => put(tx))
      }
      .flatTap(txs => Logger[F].debug(s"ConsensusService returningToPending with hashes=${txs.map(_.hash)}"))

  def getLast20Accepted: F[List[A]] =
    accepted.getLast20()

  def findHashesByMerkleRoot(merkleRoot: String): F[Option[Seq[String]]] =
    merklePool.lookup(merkleRoot)

  def count: F[Long] =
    List(
      count(ConsensusStatus.Pending),
      count(ConsensusStatus.Arbitrary),
      count(ConsensusStatus.InConsensus),
      count(ConsensusStatus.Accepted),
      count(ConsensusStatus.Unknown)
    ).sequence.map(_.combineAll)

  def count(status: ConsensusStatus): F[Long] = status match {
    case ConsensusStatus.Pending     => pending.size()
    case ConsensusStatus.Arbitrary   => arbitrary.size()
    case ConsensusStatus.InConsensus => inConsensus.size()
    case ConsensusStatus.Accepted    => accepted.size()
    case ConsensusStatus.Unknown     => unknown.size()
  }

  def getMetricsMap: F[Map[String, Long]] =
    List(
      count(ConsensusStatus.Pending),
      count(ConsensusStatus.Arbitrary),
      count(ConsensusStatus.InConsensus),
      count(ConsensusStatus.Accepted),
      count(ConsensusStatus.Unknown)
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
