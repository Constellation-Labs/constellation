package org.constellation.storage

import cats.effect.{Concurrent, ContextShift, IO}
import cats.effect.concurrent.Semaphore
import io.chrisdavenport.log4cats.Logger
import cats.implicits._
import org.constellation.ConstellationContextShift
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

  val semaphore: Semaphore[F] = {
    implicit val cs: ContextShift[IO] = ConstellationContextShift.edge
    Semaphore.in[IO, F](1).unsafeRunSync
  }

  protected def withLock[R](name: String, thunk: F[R]): F[R] = new SingleLock[F, R](name, semaphore).use(thunk)

  protected[storage] val pending: PendingMemPool[F, A]
  protected[storage] val arbitrary = new StorageService[F, A](Some(240))
  protected[storage] val inConsensus = new StorageService[F, A](Some(240))
  protected[storage] val accepted = new StorageService[F, A](Some(240))
  protected[storage] val unknown = new StorageService[F, A](Some(240))

  def getArbitrary: F[Map[String, A]] = arbitrary.toMap()

  def put(a: A): F[A] = put(a, ConsensusStatus.Pending)

  def put(a: A, as: ConsensusStatus): F[A] = as match {
    case ConsensusStatus.Pending   => withLock("pendingPut", pending.put(a.hash, a))
    case ConsensusStatus.Arbitrary => withLock("arbitraryPut", arbitrary.put(a.hash, a))
    case ConsensusStatus.Accepted  => withLock("acceptedPut", accepted.put(a.hash, a))
    case ConsensusStatus.Unknown   => withLock("unknownPut", unknown.put(a.hash, a))
    case _                         => new Exception("Unknown consensus status").raiseError[F, A]
  }

  def update(key: String, fn: A => A): F[Unit] =
    for {
      _ <- withLock("pendingUpdate", pending.update(key, fn))
      _ <- withLock("arbitraryUpdate", arbitrary.update(key, fn))
      _ <- withLock("inConsensusUpdate", inConsensus.update(key, fn))
      _ <- withLock("acceptedUpdate", accepted.update(key, fn))
      _ <- withLock("unknownUpdate", unknown.update(key, fn))
    } yield ()

  def accept(a: A): F[Unit] =
    put(a, ConsensusStatus.Accepted) *>
      withLock("inConsensusRemove", inConsensus.remove(a.hash)) *>
      withLock("unknownRemove", unknown.remove(a.hash)) *>
      withLock("arbitraryRemove", arbitrary.remove(a.hash))

  def isAccepted(hash: String): F[Boolean] = accepted.contains(hash)

  def pullForConsensus(minCount: Int): F[List[A]] =
    withLock("pullForConsensus", pending.pull(minCount))
      .map(_.getOrElse(List()))
      .flatMap(_.traverse(a => withLock("inConsensusPut", inConsensus.put(a.hash, a))))

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

  def returnToPending(as: Seq[String]): F[List[A]] =
    as.toList
      .traverse(inConsensus.lookup)
      .map(_.flatten)
      .flatMap { txs =>
        txs.traverse(tx => withLock("inConsensusRemove", inConsensus.remove(tx.hash))) *>
          txs.traverse(tx => put(tx))
      }

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
