package org.constellation.primitives.concurrency
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, Timer}
import cats.implicits._

import scala.concurrent.duration._

class SingleLock[F[_], R](name: String, s: Semaphore[F])(op: => F[R])(implicit F: Concurrent[F]){
  def use: F[R] = {
    for {
      x <- s.available
      _ <- F.delay(println(s"$name >> Availability: $x"))
      _ <- s.acquire
      y <- s.available
      _ <- F.delay(println(s"$name >> Started | Availability: $y"))
      res <- op
      _ <- s.release
      z <- s.available
      _ <- F.delay(println(s"$name >> Done | Availability: $z"))
    } yield res
  }
}