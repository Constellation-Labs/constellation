package org.constellation.primitives.concurrency

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Concurrent, IO}

class SingleRef[F[_]: Concurrent, A](default: A, s: Semaphore[F] = null) {

  private val ref = Ref.unsafe[F, A](default)
  private val semaphore: Semaphore[F] = Semaphore.in[IO, F](1).unsafeRunSync()

  val lock = new SingleLock[F, A]("", if (s != null) s else semaphore)

  def acquire: F[Unit] = lock.acquire
  def release: F[Unit] = lock.release

  def get: F[A] = withLock(ref.get)
  def getUnsafe: F[A] = ref.get

  def set(v: A): F[Unit] = withLock(ref.set(v))
  def setUnsafe(v: A): F[Unit] = ref.set(v)

  def update(f: A => A): F[Unit] = withLock(ref.update(f))
  def updateUnsafe(f: A => A): F[Unit] = (ref.update(f))

  def modify[B](f: A => (A, B)): F[B] = withLock(ref.modify(f))
  def unsafeModify[B](f: A => (A, B)): F[B] = ref.modify(f)

  private def withLock[R](thunk: => F[R]): F[R] = new SingleLock[F, R]("", if (s != null) s else semaphore).use(thunk)

}

object SingleRef {

  def apply[F[_]: Concurrent, A](default: A, semaphore: Semaphore[F] = null): SingleRef[F, A] =
    new SingleRef[F, A](default, semaphore)
}
