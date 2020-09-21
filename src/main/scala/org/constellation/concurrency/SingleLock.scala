package org.constellation.concurrency

import cats.effect.Concurrent
import cats.effect.concurrent.Semaphore
import cats.syntax.all._

class SingleLock[F[_], R](name: String, lock: Semaphore[F])(implicit F: Concurrent[F]) {

  def acquire: F[Unit] = lock.acquire
  def release: F[Unit] = lock.release

  def use(thunk: => F[R]): F[R] =
    for {
      _ <- lock.acquire

      res <- thunk.handleErrorWith { error =>
        lock.release >>
          error.raiseError[F, R]
      }

      _ <- lock.release
    } yield res

}
