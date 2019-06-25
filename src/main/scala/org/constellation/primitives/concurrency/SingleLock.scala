package org.constellation.primitives.concurrency

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.Concurrent
import cats.implicits._

class SingleLock[F[_], R](name: String, lock: Semaphore[F])(implicit F: Concurrent[F]) {

  def use(thunk: => F[R]): F[R] =
    for {
      _ <- lock.acquire

      res <- thunk.handleErrorWith { error =>
        lock.release *>
          error.raiseError[F, R]
      }

      _ <- lock.release
    } yield res

}
