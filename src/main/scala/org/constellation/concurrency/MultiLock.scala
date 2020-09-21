package org.constellation.concurrency

import cats.effect.Concurrent
import cats.syntax.all._

import scala.collection.concurrent.TrieMap

class MultiLock[F[_]: Concurrent, K] {

  import cats.effect.concurrent.Semaphore

  private[this] val locks = TrieMap.empty[K, Semaphore[F]]

  def acquire[R](keys: List[K])(thunk: => F[R])(implicit o: Ordering[K]): F[R] = {
    def lockAll() =
      for {
        k <- keys.sorted.pure[F]

        semaphores <- k.map { ks =>
          Semaphore[F](1).map(locks.getOrElseUpdate(ks, _))
        }.sequence

        openLocks <- semaphores
          .map(ss => {
            ss.acquire.map(_ => ss)
          })
          .sequence
      } yield openLocks

    def unlockAll(openLocks: List[Semaphore[F]]) =
      openLocks.map(_.release).sequence

    for {
      openLocks <- lockAll()
      result <- thunk
      _ <- unlockAll(openLocks)
    } yield result
  }
}
