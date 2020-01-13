package org.constellation.storage

import cats.effect.Concurrent
import cats.effect.concurrent.Semaphore
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext
import org.constellation.primitives.Schema.SignedObservationEdge

class SOEService[F[_]: Concurrent]() {

  private val logger = Slf4jLogger.getLogger[F]

  private val semaphore: Semaphore[F] = ConstellationExecutionContext.createSemaphore()
  private val memPool = new ConcurrentStorageService[F, SignedObservationEdge](semaphore, "SoeMemPool".some, 120.some)

  def lookup(key: String): F[Option[SignedObservationEdge]] =
    memPool.lookup(key)

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

  def applySnapshot(soeHashes: List[String]): F[Unit] =
    soeHashes.traverse(memPool.remove).void

  def clear: F[Unit] =
    memPool.clear
      .flatTap(_ => logger.info("SOEService has been cleared"))
}
