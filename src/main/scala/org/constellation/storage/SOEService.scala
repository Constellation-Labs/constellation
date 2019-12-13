package org.constellation.storage

import java.util.concurrent.TimeUnit

import cats.effect.concurrent.Semaphore
import cats.effect.Concurrent
import cats.implicits._
import org.constellation.primitives.Schema.SignedObservationEdge
import org.constellation.{ConfigUtil, ConstellationExecutionContext}

class SOEService[F[_]: Concurrent]() {

  private val semaphore: Semaphore[F] = ConstellationExecutionContext.createSemaphore()

  private val memPool = new ConcurrentStorageService[F, SignedObservationEdge](
    semaphore,
    "SoeMemPool".some,
    ConfigUtil.constellation.getDuration("storage.soe.memPoolExpiration", TimeUnit.MINUTES).toInt.some
  )
  def lookup(key: String): F[Option[SignedObservationEdge]] = memPool.lookup(key)

  def put(key: String, value: SignedObservationEdge): F[SignedObservationEdge] =
    memPool.put(key, value)

  def update(
    key: String,
    updateFunc: SignedObservationEdge => SignedObservationEdge,
    empty: => SignedObservationEdge
  ): F[SignedObservationEdge] =
    memPool.update(key, updateFunc, empty)

  def update(
    key: String,
    updateFunc: SignedObservationEdge => SignedObservationEdge
  ): F[Option[SignedObservationEdge]] =
    memPool.update(key, updateFunc)
}
