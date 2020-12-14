package org.constellation.storage

import cats.effect.Concurrent
import cats.effect.concurrent.Semaphore
import org.constellation.concurrency.SingleLock

class ConcurrentStorageService[F[_]: Concurrent, V](
  semaphore: Semaphore[F],
  metricName: Option[String]
) extends StorageService[F, V](metricName) {

  private def withLock[R](thunk: F[R]) = new SingleLock[F, R]("ConcurrentStorageService", semaphore).use(thunk)

  override def putToCache(key: String, v: V): F[V] =
    withLock(super.putToCache(key, v))
}
