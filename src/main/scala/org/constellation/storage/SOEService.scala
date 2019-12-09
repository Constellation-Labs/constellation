package org.constellation.storage

import java.util.concurrent.TimeUnit

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, IO}
import cats.implicits._
import org.constellation.primitives.Schema.SignedObservationEdgeCache
import org.constellation.{ConfigUtil, ConstellationExecutionContext}

class SOEService[F[_]: Concurrent]() {

  private val semaphore: Semaphore[F] = ConstellationExecutionContext.createSemaphore()

  private val memPool = new ConcurrentStorageService[F, SignedObservationEdgeCache](
    semaphore,
    "SoeMemPool".some,
    ConfigUtil.constellation.getDuration("storage.soe.memPoolExpiration", TimeUnit.MINUTES).toInt.some
  )
  def lookup(key: String): F[Option[SignedObservationEdgeCache]] = memPool.lookup(key)

  def put(key: String, value: SignedObservationEdgeCache): F[SignedObservationEdgeCache] =
    memPool.put(key, value)

  def update(
    key: String,
    updateFunc: SignedObservationEdgeCache => SignedObservationEdgeCache,
    empty: => SignedObservationEdgeCache
  ): F[SignedObservationEdgeCache] =
    memPool.update(key, updateFunc, empty)

  def update(
    key: String,
    updateFunc: SignedObservationEdgeCache => SignedObservationEdgeCache
  ): F[Option[SignedObservationEdgeCache]] =
    memPool.update(key, updateFunc)
}
