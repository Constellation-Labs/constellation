package org.constellation.primitives.concurrency

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.Concurrent
import cats.implicits._

class SingleLock[F[_], R](name: String, lock: Semaphore[F])(implicit F: Concurrent[F]) {

  def use(thunk: => F[R]): F[R] =
    for {
      x <- lock.available
      _ <- Concurrent[F].delay(println(s"[$name] Availability: $x"))

      _ <- lock.acquire

      y <- lock.available
      _ <- Concurrent[F].delay(println(s"[$name] Started | Availability: $y"))

      res <- thunk.handleErrorWith { error =>
        lock.release *>
          error.raiseError[F, R]
      }

      _ <- lock.release

      z <- lock.available
      _ <- Concurrent[F].delay(println(s"[$name] Done | Availability: $z"))
    } yield res

}
