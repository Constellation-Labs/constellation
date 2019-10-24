package org.constellation.storage

import cats.effect.Concurrent
import cats.effect.concurrent.Semaphore
import org.constellation.primitives.concurrency.SingleLock

class ConcurrentStorageService[F[_]: Concurrent, V](
  semaphore: Semaphore[F],
  metricName: Option[String],
  expireAfterMinutes: Option[Int] = Some(240)
) extends StorageService[F, V](metricName, expireAfterMinutes) {

  private def withLock[R](thunk: F[R]) = new SingleLock[F, R]("ConcurrentStorageService", semaphore).use(thunk)

  override def putToCache(key: String, v: V): F[V] =
    withLock(super.putToCache(key, v))
}
